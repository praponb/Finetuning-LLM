from pyspark.sql import functions as F

valid = (
    df_orders
    .filter((F.col("order_date") >= F.lit("2023-02-23").cast("date")) & F.col("store_id").isNotNull())
    .select("*", F.lower("status").alias("status_lower"))
)

result = (
    valid
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "inner")
    .rollup(
        F.col("p.segment").alias("segment"),
        F.col("p.city").alias("city"),
        F.col("s.status").alias("status"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.min(F.col("s.discount")).alias("min_discount"),
        F.sum(F.col("s.shipping_fee")).alias("total_shipping_fee"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy(F.col("num_rows").desc_nulls_first())
)
