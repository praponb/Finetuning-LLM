from pyspark.sql import functions as F

claim_rows = (
    df_claims
    .filter((F.hour(F.col("reported_at")).between(9, 11)) & (F.col("claimed_amount") >= 4868.7))
    .select(
        F.lit("claim").alias("record_type"),
        "policy_id",
        F.col("reported_at").alias("event_time"),
    )
)

claim_side = (
    df_claims
    .filter((F.col("claim_type") == "fire") & (F.col("status").isNotNull()))
    .select(
        F.lit("claim_2").alias("record_type"),
        F.col("adjuster_id").alias("policy_id"),
        F.col("reported_at").alias("event_time"),
    )
)

result = (
    claim_rows
    .unionByName(claim_side)
    .orderBy("policy_id", "record_type")
)
