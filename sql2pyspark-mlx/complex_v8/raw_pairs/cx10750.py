from pyspark.sql import functions as F

base_policys = (
    df_policies
    .filter((F.col("start_date").between(F.lit("2024-07-19").cast("date"), F.lit("2025-05-28").cast("date"))) & (F.col("status").isin("CANCELLED", "LAPSED")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.add_months(F.col("end_date"), 12).alias("end_date_plus_12m"),
        F.when(F.col("premium") >= 3521.8, "large").when(F.col("premium") >= 2096.6, "medium").otherwise("small").alias("premium_tier"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.max("claim_date").alias("last_claim_date"),
        F.count("*").alias("claim_count"),
        F.count(F.when(F.col("claim_type").isin("theft", "delay", "medical"), 1)).alias("claim_claim_type_count"),
    )
)

profiled = (
    base_policys.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "c.last_claim_date",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        F.coalesce(F.col("c.claim_claim_type_count"), F.lit(0)).alias("claim_claim_type_count"),
    )
)

result = (
    profiled
    .rollup("premium_tier", "claim_count", "claim_claim_type_count")
    .agg(
        F.grouping_id().alias("gid"),
        F.min("premium").alias("min_premium"),
        F.array_sort(F.collect_set("product")).alias("product_list"),
    )
)
