from pyspark.sql import functions as F

cleaned_claims = (
    df_claims
    .filter(F.col("claimed_amount") >= 18778.6)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("claim_date")).alias("days_since_claim_date"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("claim_date")).alias("days_since_claim_date_2"),
        F.abs(F.col("claimed_amount") - 32441.2).alias("claimed_amount_distance"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.count(F.when(~F.col("claim_type").isin("fire", "medical"), 1)).alias("claim_claim_type_count"),
        F.max("reported_at").alias("last_claim_reported_at"),
        F.count("*").alias("claim_count"),
    )
)

summarized_rows = (
    cleaned_claims.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_claim_type_count"), F.lit(0)).alias("claim_claim_type_count"),
        "c.last_claim_reported_at",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
    )
)

claim_stats_2 = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.count("*").alias("claim_count_2"),
        F.min("reported_at").alias("first_claim_reported_at"),
        F.min("approved_amount").alias("min_claim_approved_amount"),
    )
)

with_counts = (
    summarized_rows.alias("s")
    .join(claim_stats_2.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count_2"), F.lit(0)).alias("claim_count_2"),
        "c.first_claim_reported_at",
        "c.min_claim_approved_amount",
    )
)

combined_details = (
    with_counts.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.status",
        "s.reported_at",
        "s.days_since_claim_date",
        "s.days_since_claim_date_2",
        "s.claimed_amount_distance",
        "s.claim_claim_type_count",
        "s.claim_count",
        "s.claim_count_2",
        "s.first_claim_reported_at",
        "s.min_claim_approved_amount",
        F.col("p.product").alias("policy_product"),
    )
)

result = (
    combined_details
    .select("id", "claimed_amount_distance", "min_claim_approved_amount")
    .unpivot(
        ids=["id"],
        values=["claimed_amount_distance", "min_claim_approved_amount"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("id", "metric")
)
