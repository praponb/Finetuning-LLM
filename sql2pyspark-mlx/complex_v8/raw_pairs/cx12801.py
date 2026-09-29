from pyspark.sql import functions as F

cleaned = (
    df_policies
    .filter((F.col("status").isin("CANCELLED", "ACTIVE")) | (F.col("product") == "HOME"))
)

enriched = (
    cleaned
    .filter(F.col("holder_id") >= 6)
    .select(
        "*",
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
        F.upper(F.trim(F.col("status"))).alias("status_clean"),
        F.substring(F.trim(F.col("product")), 1, 4).alias("product_prefix"),
    )
)

with_details_holder = (
    enriched.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "s.premium_filled",
        "s.status_clean",
        "s.product_prefix",
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

result = (
    with_details_holder
    .cube("product", "holder_id")
    .agg(
        F.countDistinct("holder_id").alias("distinct_holder_ids"),
        F.sum("premium").alias("total_premium"),
    )
    .select(
        F.coalesce(F.col("product"), F.lit("ALL")).alias("product"),
        "holder_id",
        "distinct_holder_ids",
        "total_premium",
    )
)
