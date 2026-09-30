from pyspark.sql import functions as F

claim_rows = (
    df_claims
    .select(
        F.col("policy_id").alias("entity_id"),
        F.col("adjuster_id").alias("key_id"),
        F.col("status").alias("category"),
    )
)

adjuster_rows = (
    df_adjusters
    .select(
        F.col("level").alias("entity_id"),
        F.col("id").alias("key_id"),
        F.col("team").alias("category"),
    )
)

result = (
    claim_rows
    .unionByName(adjuster_rows)
    .orderBy(F.col("entity_id").desc())
)
