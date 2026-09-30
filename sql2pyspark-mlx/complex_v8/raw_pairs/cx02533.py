from pyspark.sql import functions as F

scoped_claims = (
    df_claims
    .filter((F.col("reported_at") < F.lit("2025-03-02 12:00:00").cast("timestamp")) & (F.col("policy_id") >= 5))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "status",
        "reported_at",
        (F.floor(F.col("approved_amount") / 10000) * 10000).alias("approved_amount_bucket"),
        F.when(F.col("approved_amount") >= 23120.7, "A").when(F.col("approved_amount") >= 2349.3, "B").otherwise("C").alias("approved_amount_tier"),
    )
)

annotated = (
    scoped_claims
    .filter(F.to_date(F.col("reported_at")) < F.lit("2025-02-18").cast("date"))
    .select(
        "*",
        F.coalesce(F.col("approved_amount_bucket"), F.lit(0)).alias("approved_amount_bucket_filled"),
        F.upper(F.trim(F.col("status"))).alias("status_clean"),
    )
)

enriched = (
    annotated.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.reported_at",
        "s.approved_amount_tier",
        F.col("p.end_date").alias("policy_end_date"),
        "a.adjuster_name",
    )
)

result = (
    enriched
    .filter(F.col("approved_amount_tier") != "A")
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "reported_at",
        "approved_amount_tier",
        "policy_end_date",
        "adjuster_name",
        F.substring(F.trim(F.col("approved_amount_tier")), 1, 3).alias("approved_amount_tier_prefix"),
    )
    .orderBy("adjuster_id", F.col("reported_at").desc())
)
