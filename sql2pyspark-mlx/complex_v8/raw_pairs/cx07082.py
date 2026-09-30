from pyspark.sql import functions as F

claim_side = (
    df_claims
    .filter(F.col("claim_type").isNotNull())
    .select(
        F.col("id").alias("ref_id"),
        "claim_type",
        F.col("reported_at").alias("happened_at"),
        F.lit("claim").alias("record_type"),
    )
)

claim_side_2 = (
    df_claims
    .select(
        F.col("id").alias("ref_id"),
        F.col("status").alias("claim_type"),
        F.col("reported_at").alias("happened_at"),
        F.lit("claim_2").alias("record_type"),
    )
)

result = claim_side.union(claim_side_2)
