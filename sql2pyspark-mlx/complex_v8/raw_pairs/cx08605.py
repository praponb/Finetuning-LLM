from pyspark.sql import functions as F

filtered = (
    df_adjusters
    .filter(F.length(F.trim(F.col("adjuster_name"))) >= 8)
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
    )
)

adjuster_side = (
    filtered
    .filter(F.upper(F.trim(F.col("adjuster_name"))).like("H%"))
    .select(
        F.lit("adjuster").alias("record_type"),
        F.col("id").alias("entity_id"),
        F.col("adjuster_name").alias("label"),
    )
)

claim_rows = (
    df_claims
    .filter((F.col("approved_amount").between(0.0, 32878.5)) & (F.col("claim_date").between(F.lit("2024-03-16").cast("date"), F.lit("2025-04-17").cast("date"))))
    .select(
        F.lit("claim").alias("record_type"),
        F.col("adjuster_id").alias("entity_id"),
        F.col("claim_type").alias("label"),
    )
)

claim_side = (
    df_claims
    .select(
        F.lit("claim").alias("record_type"),
        F.col("id").alias("entity_id"),
        F.col("claim_type").alias("label"),
    )
)

result = (
    adjuster_side
    .unionByName(claim_rows)
    .unionByName(claim_side)
)
