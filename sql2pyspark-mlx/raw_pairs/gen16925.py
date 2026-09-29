from pyspark.sql import functions as F

exploded = (
    df_carts
    .select("id", "customer_id", F.explode_outer("attributes").alias("k", "attr_value"))
)

enriched = (
    exploded
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.segment"), F.col("p.name"), F.col("p.credit_limit"))
)

base = (
    enriched
    .filter(F.col("segment").isNotNull() & F.col("name").like("O%"))
    .select(
        "*",
        F.ceil("credit_limit").alias("credit_limit_whole"),
        F.coalesce("segment", F.lit("UNKNOWN")).alias("resolved_segment"),
    )
)

base_carts = (
    base
    .filter(~F.col("k").eqNullSafe(F.lit("utm_source")))
    .select(
        "id",
        "customer_id",
        "name",
        "credit_limit",
        "resolved_segment",
        F.initcap(F.trim("k")).alias("k_title"),
    )
)

result = (
    base_carts
    .select("id", "customer_id", "credit_limit")
    .orderBy(F.col("credit_limit").desc_nulls_first())
)
