from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("cost", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("status").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("channel").orderBy("cost", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.partitionBy("channel").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("order_id").orderBy("cost_avg_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("shipping_fee_avg_2")
w_running = Window.partitionBy("status").orderBy("shipping_fee_avg_2_cume_dist", "id")
w8 = Window.partitionBy("order_id").orderBy(F.col("cost").asc_nulls_first())
w9 = Window.partitionBy("order_id").orderBy("running_shipping_fee_avg_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("order_id").orderBy("cumulative_cost", "id").rowsBetween(-3, Window.currentRow)
w11 = Window.partitionBy("channel").orderBy("shipping_fee_avg_2", "id")

enriched = (
    df_shipments
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "inner")
    .select("s.*", F.col("p.shipping_fee"), F.col("p.channel"))
)

with_windows = (
    enriched
    .select(
        "*",
        F.avg("shipping_fee").over(w).alias("shipping_fee_avg_2"),
        F.sum("cost").over(w2).alias("cumulative_cost"),
        F.avg("cost").over(w3).alias("cost_avg_2"),
    )
)

joined = (
    with_windows
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "inner")
    .select("src.*", F.col("b.amount"))
)

enriched_shipments = (
    joined
    .withColumn("running_shipping_fee_avg_2", F.sum("shipping_fee_avg_2").over(w4))
    .withColumn("last_known_carrier", F.last("carrier", ignorenulls=True).over(w5))
    .withColumn("shipping_fee_avg_2_cume_dist", F.cume_dist().over(w6))
)

metrics = (
    enriched_shipments
    .withColumn("shipping_fee_avg_2_cume_dist_bucket", F.ntile(4).over(w_running))
    .withColumn("cost_dense_rank", F.dense_rank().over(w8))
)

filtered = (
    metrics
    .filter(F.col("shipping_fee_avg_2_cume_dist_bucket") != 7)
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "shipping_fee",
        "channel",
        "shipping_fee_avg_2",
        "cumulative_cost",
        "cost_avg_2",
        "amount",
        "running_shipping_fee_avg_2",
        "shipping_fee_avg_2_cume_dist",
        "shipping_fee_avg_2_cume_dist_bucket",
        "cost_dense_rank",
        F.last_day("shipped_date").alias("shipped_date_month_end"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("channel_clean"),
    )
)

result = (
    filtered
    .select(
        "*",
        F.first("amount", ignorenulls=True).over(w9).alias("first_known_amount"),
        F.avg("cumulative_cost").over(w10).alias("cumulative_cost_avg_3"),
        F.lead("shipping_fee", 1).over(w11).alias("next_shipping_fee"),
    )
)
