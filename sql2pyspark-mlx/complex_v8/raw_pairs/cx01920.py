from pyspark.sql import functions as F

cleaned_claims = (
    df_claims
    .filter(F.col("approved_amount") <= 14993.0)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "approved_amount",
        "reported_at",
        F.date_format(F.col("reported_at"), "MMM yyyy").alias("reported_at_label"),
        F.concat(F.col("claim_type"), F.lit("#"), F.col("id").cast("string")).alias("claim_type_ref"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(F.sum("claimed_amount").alias("total_claim_claimed_amount"))
)

with_stats = (
    cleaned_claims.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select("s.*", "c.total_claim_claimed_amount")
)

derived = (
    with_stats
    .withColumn("days_since_claim_date", F.datediff(F.lit("2025-06-30").cast("date"), F.col("claim_date")))
)

result = (
    derived
    .rollup("adjuster_id", "policy_id")
    .agg(
        F.grouping("adjuster_id").alias("is_total_adjuster_id"),
        F.max("days_since_claim_date").alias("max_days_since_claim_date"),
    )
    .orderBy(
        F.col("adjuster_id").asc_nulls_last(),
        F.col("policy_id").asc_nulls_last(),
    )
)
