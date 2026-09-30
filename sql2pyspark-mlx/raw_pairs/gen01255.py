from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("key", "v"),
    )
)

enriched = (
    lines
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "left")
    .select(
        "src.customer_id",
        "src.created_at",
        "src.coupon_codes",
        "src.key",
        "src.v",
        F.col("d.email"),
        F.col("d.country"),
    )
)

joined = (
    enriched
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "inner")
    .select("a.customer_id", "a.coupon_codes", "a.country", F.col("p.name"))
)

enriched_carts = (
    joined
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id").eqNullSafe(F.col("p.id")), "left")
    .select("src.*", F.col("p.age"))
)

result = (
    enriched_carts
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "inner")
    .rollup(
        F.col("p.segment").alias("segment"),
        F.col("src.country").alias("cart_country"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.col("src.age")).alias("total_age"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy(F.col("total_age").desc_nulls_last())
)
