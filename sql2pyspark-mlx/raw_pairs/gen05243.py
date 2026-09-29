from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("is_cash").asc_nulls_first(), "id")
w2 = Window.orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("shipping_fee", "id").rowsBetween(-5, Window.currentRow)
w_grp = Window.orderBy(F.col("shipping_fee").asc_nulls_last())
w5 = Window.orderBy(F.col("shipping_fee").desc(), "id")
w6 = Window.partitionBy("shipping_fee_tier").orderBy(F.col("max_last_known_shipping_fee").desc())

valid = (
    df_payments
    .filter(F.col("paid_at") > F.lit("2023-10-20").cast("date"))
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.when(F.col("method") == "cash", 1).otherwise(0).alias("is_cash"),
        F.coalesce("method", F.lit("N/A")).alias("method_clean"),
    )
)

latest = (
    valid
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

with_dims = (
    latest
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.amount") >= 480)
    .select(
        "e.id",
        "e.order_id",
        "e.method",
        "e.paid_at",
        "e.status",
        "e.row_num",
        F.col("o.status").alias("order_status"),
        F.col("o.order_date"),
    )
)

enriched = (
    with_dims
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id") == F.col("o.id"), "inner")
    .select("a.id", "a.order_id", "a.status", "a.order_status", F.col("o.shipping_fee"))
)

cleaned = (
    enriched
    .filter(F.col("order_id").isNotNull())
    .select(
        "id",
        "shipping_fee",
        F.coalesce("order_status", F.lit("N/A")).alias("order_status_filled"),
        F.initcap(F.trim("order_status")).alias("order_status_title"),
        F.coalesce("order_status", F.lit("NONE")).alias("resolved_order_status"),
    )
)

scored = (
    cleaned
    .select(
        "*",
        F.last("shipping_fee", ignorenulls=True).over(w2).alias("last_known_shipping_fee"),
        F.avg("shipping_fee").over(w3).alias("shipping_fee_avg_5"),
        F.cume_dist().over(w_grp).alias("shipping_fee_cume_dist"),
        F.ntile(10).over(w5).alias("shipping_fee_bucket"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("shipping_fee") >= 44, "HIGH").otherwise("LOW").alias("shipping_fee_tier"),
        F.when(F.col("shipping_fee_cume_dist") >= 0.67, "PREMIUM").otherwise("STANDARD").alias("shipping_fee_cume_dist_tier"),
    )
    .agg(
        F.max("last_known_shipping_fee").alias("max_last_known_shipping_fee"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("shipping_fee", [0.25, 0.5, 0.75]).alias("shipping_fee_quartiles"),
        F.round(F.avg("shipping_fee_cume_dist"), 2).alias("avg_shipping_fee_cume_dist"),
        F.avg("shipping_fee_cume_dist").alias("avg_shipping_fee_cume_dist_2"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("avg_shipping_fee_cume_dist_2") > 0)
    .withColumn("max_last_known_shipping_fee_rank", F.rank().over(w6))
    .orderBy(F.col("shipping_fee_tier").asc_nulls_last())
)
