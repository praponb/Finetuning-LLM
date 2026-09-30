from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id", "carrier").orderBy("shipping_fee", "id")
w3 = Window.partitionBy("status").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("shipping_fee").desc(), "id")
w_grp = Window.partitionBy("order_id", "order_status")
w_hist = Window.partitionBy("order_status").orderBy(F.col("discount").desc_nulls_last())
w7 = Window.orderBy(F.col("shipping_fee").desc())

enriched = (
    df_shipments
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "left")
    .select(
        "src.*",
        F.col("o.status").alias("order_status"),
        F.col("o.amount"),
        F.col("o.shipping_fee"),
    )
)

with_windows = (
    enriched
    .withColumn("first_known_order_status", F.first("order_status", ignorenulls=True).over(w))
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("running_cost", F.sum("cost").over(w3))
    .withColumn("shipping_fee_bucket", F.ntile(4).over(w4))
)

enriched_shipments = (
    with_windows
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "inner")
    .select(
        "a.*",
        F.col("d.status").alias("order_status_2"),
        F.col("d.discount"),
        F.col("d.channel"),
    )
)

kept_keys = (
    enriched_shipments
    .select("order_id", "carrier")
    .exceptAll(
        df_shipments
        .filter(F.col("delivered_date") < "2024-12-01")
        .select("order_id", "carrier")
    )
)

kept_rows = (
    enriched_shipments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.carrier") == F.col("k.carrier")), "inner")
    .select("s.*")
)

valid = (
    kept_rows
    .filter(F.col("delivered_date") >= F.lit("2023-04-29").cast("date"))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "order_status",
        "amount",
        "shipping_fee",
        "row_seq",
        "running_cost",
        "discount",
        F.col("amount").cast("int").alias("amount_int"),
        F.coalesce("carrier", F.lit("NONE")).alias("resolved_carrier"),
    )
)

scored = (
    valid
    .select(
        "*",
        F.count("cost").over(w_grp).alias("order_id_count_cost"),
        F.rank().over(w_hist).alias("discount_rank"),
        F.rank().over(w7).alias("shipping_fee_rank"),
    )
)

joined = (
    scored
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.status"))
)

result = (
    joined
    .groupBy(
        F.when(F.col("shipping_fee") >= 36, "PREMIUM").otherwise("STANDARD").alias("shipping_fee_tier"),
        "order_status",
    )
    .agg(
        F.countDistinct("carrier").alias("distinct_carriers"),
        F.count_if(F.col("order_status") == "SHIPPED").alias("shipped_events"),
        F.sum(F.when(F.col("carrier") == "UPS", F.col("discount")).otherwise(0)).alias("ups_discount"),
        F.stddev_samp("cost").alias("cost_stddev"),
    )
)
