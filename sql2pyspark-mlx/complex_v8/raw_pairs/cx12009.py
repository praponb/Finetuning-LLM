from pyspark.sql import functions as F

claim_side = (
    df_claims
    .select(
        "status",
        F.col("reported_at").alias("event_time"),
        "claim_type",
        F.lit("claim").alias("record_type"),
    )
)

claim_side_2 = (
    df_claims
    .select(
        "claim_type",
        "reported_at",
        "status",
        F.lit("claim_2").alias("record_type"),
    )
)

result = claim_side.union(claim_side_2)
