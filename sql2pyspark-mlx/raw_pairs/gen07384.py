from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "order_id").orderBy(F.col("delivered_date").asc_nulls_first(), "id")
w2 = Window.partitionBy("channel", "status").orderBy(F.col("amount").desc(), "id")
w3 = Window.partitionBy(F.date_trunc("week", "order_order_date"), "status").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("channel").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.year("shipped_date"), "channel").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("channel").orderBy("running_shipping_fee", "id").rowsBetween(-6, Window.currentRow)

base = (
    df_shipments
    .filter((F.col("shipped_date") >= F.lit("2023-04-15").cast("date")) & (~F.col("carrier").eqNullSafe(F.lit("Kerry"))))
    .select(
        "*",
        F.when(F.col("carrier") == "UPS", 1).otherwise(0).alias("is_ups"),
        F.coalesce("carrier", F.lit("DIRECT")).alias("carrier_clean"),
    )
)

enriched = (
    base
    .alias("s")
    .join(df_orders.alias("b"), F.col("s.order_id").eqNullSafe(F.col("b.id")), "inner")
    .filter(F.col("b.amount") >= 900)
    .select("s.*", F.col("b.amount"), F.col("b.order_date"), F.col("b.shipping_fee"))
)

joined = (
    enriched
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id").eqNullSafe(F.col("d.id")), "inner")
    .select(
        "src.id",
        "src.order_id",
        "src.shipped_date",
        "src.delivered_date",
        "src.status",
        "src.carrier_clean",
        "src.amount",
        "src.shipping_fee",
        F.col("d.order_date").alias("order_order_date"),
        F.col("d.channel"),
    )
)

latest = (
    joined
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

with_windows = (
    latest
    .select(
        "id",
        "delivered_date",
        "carrier_clean",
        "channel",
        F.ntile(3).over(w2).alias("amount_bucket"),
        F.sum("amount").over(w3).alias("running_amount"),
        F.first("amount", ignorenulls=True).over(w4).alias("first_known_amount"),
        F.sum("shipping_fee").over(w5).alias("running_shipping_fee"),
    )
)

metrics = (
    with_windows
    .withColumn("last7_min_running_shipping_fee", F.min("running_shipping_fee").over(w_part))
    .withColumn("last7_sum_running_shipping_fee", F.sum("running_shipping_fee").over(w_part))
    .withColumn("last7_max_running_shipping_fee", F.max("running_shipping_fee").over(w_part))
)

filtered = (
    metrics
    .filter(F.col("last7_sum_running_shipping_fee") < 600)
    .withColumn("last7_min_running_shipping_fee_int", F.col("last7_min_running_shipping_fee").cast("int"))
    .withColumn("resolved_channel", F.coalesce("channel", F.lit("N/A")))
)

stats = (
    filtered
    .groupBy(F.date_trunc("WEEK", "delivered_date").alias("week_start"))
    .agg(
        F.sum(F.when(F.col("channel") == "mobile", F.col("running_amount")).otherwise(0)).alias("mobile_running_amount"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.percentile_approx("last7_min_running_shipping_fee", [0.25, 0.5, 0.75]).alias("last7_min_running_shipping_fee_quartiles"),
        F.count_if(F.col("channel") == "web").alias("web_events"),
        F.avg(F.when(F.col("channel") == "mobile", F.col("running_shipping_fee"))).alias("mobile_avg_running_shipping_fee"),
    )
)

result = (
    stats
    .select("week_start", "mobile_running_amount", "web_events")
    .orderBy(F.col("web_events").asc_nulls_last())
)
