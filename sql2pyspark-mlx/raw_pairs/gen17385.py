from pyspark.sql import functions as F

base = (
    df_payments
    .filter((F.col("paid_at") >= F.lit("2023-08-09 20:00:00").cast("timestamp")) & F.col("method").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "status",
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
        F.initcap(F.trim("method")).alias("method_title"),
        F.coalesce("method", F.lit("N/A")).alias("method_clean"),
    )
)

with_dims = (
    base
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id").eqNullSafe(F.col("p.id")), "left")
    .filter(F.col("p.order_date") > "2025-04-01")
    .select(
        "s.id",
        "s.order_id",
        "s.method",
        "s.status",
        "s.method_clean",
        F.col("p.status").alias("order_status"),
    )
)

valid = (
    with_dims
    .filter(F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "status",
        "method_clean",
        F.concat("method", F.lit(" "), "status").alias("method_status"),
    )
)

enriched = (
    valid
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id") == F.col("b.id"), "inner")
    .select(
        "e.id",
        "e.order_id",
        F.col("b.amount"),
        F.col("b.discount"),
        F.col("b.order_date"),
    )
)

result = (
    enriched
    .rollup("order_id")
    .agg(
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.percentile_approx("amount", 0.5).alias("amount_p50"),
        F.round(F.avg("amount"), 2).alias("avg_amount_2"),
        F.grouping("order_id").alias("grouping_id"),
    )
)
