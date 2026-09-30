from pyspark.sql import functions as F

holder_side = (
    df_policyholders
    .select(
        F.col("id").alias("key_id"),
        "region",
        F.col("joined_on").alias("event_date"),
    )
)

claim_side = (
    df_claims
    .select(
        F.col("id").alias("key_id"),
        F.col("status").alias("region"),
        F.col("claim_date").alias("event_date"),
    )
)

result = holder_side.unionByName(claim_side)
