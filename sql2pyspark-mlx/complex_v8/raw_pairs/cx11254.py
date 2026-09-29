from pyspark.sql import functions as F

base = (
    df_policies
    .filter((F.col("status").isin("ACTIVE", "LAPSED")) & (F.col("premium") <= 3817.5))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        F.round(F.col("premium") * 1.1, 2).alias("premium_adj"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("start_date")).alias("days_since_start_date"),
        F.abs(F.col("premium") - 2824.5).alias("premium_distance"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.count("*").alias("claim_count"),
        F.count(F.when(F.col("claim_type") == "theft", 1)).alias("claim_claim_type_count"),
    )
)

with_counts = (
    base.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        F.coalesce(F.col("c.claim_claim_type_count"), F.lit(0)).alias("claim_claim_type_count"),
    )
)

annotated = (
    with_counts
    .filter(F.col("end_date") < F.lit("2026-09-19").cast("date"))
    .select(
        "id",
        "holder_id",
        "product",
        "end_date",
        "premium_adj",
        "days_since_start_date",
        "claim_count",
        "claim_claim_type_count",
        F.when(F.col("days_since_start_date") >= 490, "A").when(F.col("days_since_start_date") >= 175, "B").otherwise("C").alias("days_since_start_date_tier"),
    )
)

result = (
    annotated
    .cube("product", "claim_count", "claim_claim_type_count")
    .agg(F.grouping_id().alias("gid"), F.count("*").alias("cnt"))
    .select(
        F.coalesce(F.col("product"), F.lit("ALL")).alias("product"),
        "claim_count",
        "claim_claim_type_count",
        "gid",
        "cnt",
    )
    .orderBy(
        F.col("product").asc_nulls_last(),
        F.col("claim_count").asc_nulls_last(),
        F.col("claim_claim_type_count").asc_nulls_last(),
    )
)
