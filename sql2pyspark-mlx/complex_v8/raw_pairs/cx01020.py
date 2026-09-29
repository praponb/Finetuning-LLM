from pyspark.sql import functions as F

cleaned_policys = (
    df_policies
    .filter((F.col("status").isin("CANCELLED", "ACTIVE")) & (F.col("start_date") >= F.lit("2024-12-03").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
    )
)

filtered = (
    cleaned_policys.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (~F.col("p.segment").isin("RETAIL", "VIP")) & (F.col("p.risk_score").between(0.2, 0.6)), "left_semi")
)

result = (
    filtered
    .rollup("product", "holder_id")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.count("*").alias("n_rows"),
        F.percentile_approx("premium", 0.5).alias("median_premium"),
        F.sum(F.when(~F.col("product").isin("AUTO", "HEALTH"), F.col("premium"))).alias("premium_product_not_in_auto_health"),
    )
)
