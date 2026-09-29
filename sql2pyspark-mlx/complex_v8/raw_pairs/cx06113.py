from pyspark.sql import functions as F

prepared = df_adjusters.filter(F.upper(F.trim(F.col("adjuster_name"))).like("E%"))

with_activity = (
    prepared.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.id")) & (F.col("c.claim_date") < F.lit("2024-09-08").cast("date")), "left_semi")
)

line_level_claims = (
    with_activity.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.adjuster_name",
        F.col("c.id").alias("claim_id"),
        "c.claim_type",
        F.col("c.approved_amount").alias("claim_approved_amount"),
    )
)

enriched = (
    line_level_claims.alias("s")
    .join(df_claims.alias("c"), F.col("c.id") == F.col("s.claim_id"), "left")
    .select(
        "s.id",
        "s.adjuster_name",
        "s.claim_id",
        "s.claim_approved_amount",
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
        F.col("c.claim_type").alias("claim_claim_type"),
        F.col("c.status").alias("claim_status"),
    )
)

result = (
    enriched
    .cube("claim_id", "claim_claim_type", "claim_adjuster_id")
    .agg(F.sum("claim_approved_amount").alias("total_claim_approved_amount"))
)
