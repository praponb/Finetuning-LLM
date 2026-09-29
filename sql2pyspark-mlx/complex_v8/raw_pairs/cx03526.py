from pyspark.sql import functions as F

cleaned = (
    df_policies
    .filter((F.col("holder_id").isNotNull()) & (F.col("end_date") < F.lit("2026-11-25").cast("date")))
    .select(
        "id",
        "holder_id",
        "start_date",
        "end_date",
        "status",
        F.datediff(F.col("start_date"), F.col("end_date")).alias("days_end_date_to_start_date"),
        F.coalesce(F.col("product"), F.lit("UNKNOWN")).alias("product_or_unknown"),
        F.when(F.col("premium") >= 4417.0, "large").when(F.col("premium") >= 2013.8, "medium").otherwise("small").alias("premium_tier"),
    )
)

line_level = (
    cleaned.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.claim_type").isNotNull()), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.start_date",
        "s.end_date",
        "s.status",
        "s.premium_tier",
        F.col("c.id").alias("claim_id"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
        F.col("c.reported_at").alias("claim_reported_at"),
        "c.claim_type",
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
    )
)

result = (
    line_level
    .select(
        "id",
        "start_date",
        "end_date",
        "status",
        "premium_tier",
        "claim_id",
        "claim_type",
        "claim_claimed_amount",
    )
)
