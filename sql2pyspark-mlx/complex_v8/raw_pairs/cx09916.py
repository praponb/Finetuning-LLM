from pyspark.sql import functions as F

adjuster_side = (
    df_adjusters
    .filter(F.col("adjuster_name").isNotNull())
    .select(
        "id",
        F.col("level").alias("entity_id"),
        F.lit("adjuster").alias("record_type"),
    )
)

claim_side = (
    df_claims
    .filter((F.col("claim_type").isin("flood", "delay")) & (F.col("claim_date").isNotNull()))
    .select("policy_id", "adjuster_id", F.lit("claim").alias("record_type"))
)

result = (
    adjuster_side
    .union(claim_side)
    .orderBy("id")
)
