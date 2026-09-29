from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("method")

filtered = (
    df_payments
    .filter((F.col("paid_at") > F.lit("2023-06-16").cast("date")) & F.col("order_id").isNotNull())
    .withColumn("amount_filled", F.coalesce("amount", F.lit(0)))
    .withColumn("resolved_method", F.coalesce("method", F.lit("OTHER")))
)

with_dims = (
    filtered
    .alias("s")
    .join(df_orders.alias("b"), F.col("s.order_id") == F.col("b.id"), "inner")
    .select(
        "s.id",
        "s.order_id",
        "s.method",
        "s.amount",
        "s.status",
        "s.resolved_method",
        F.col("b.status").alias("order_status"),
        F.col("b.amount").alias("order_amount"),
    )
)

with_dims_payments = (
    with_dims
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "left")
    .filter(F.col("p.amount") >= 1410)
    .select("e.*", F.col("p.shipping_fee"), F.col("p.discount"))
)

enriched = (
    with_dims_payments
    .select(
        "id",
        "order_id",
        "amount",
        "shipping_fee",
        "discount",
        F.first("order_amount", ignorenulls=True).over(w).alias("first_known_order_amount"),
        F.stddev_samp("discount").over(w2).alias("method_stddev_samp_discount"),
        F.avg("discount").over(w2).alias("method_avg_discount"),
    )
)

result = (
    enriched
    .alias("a")
    .join(df_orders.alias("dim"), F.col("a.order_id") == F.col("dim.id"), "left")
    .cube(F.col("dim.channel").alias("channel"), F.col("dim.status").alias("status"))
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("a.shipping_fee")).alias("min_shipping_fee"),
        F.round(F.avg(F.col("a.amount")), 2).alias("avg_amount"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy(F.col("channel").desc_nulls_first())
)
