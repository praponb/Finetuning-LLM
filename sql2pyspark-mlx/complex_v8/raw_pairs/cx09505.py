from pyspark.sql import functions as F

claim_side = (
    df_claims
    .select(
        "claim_type",
        F.col("claimed_amount").alias("metric"),
        F.lit("claim").alias("record_type"),
    )
)

claim_side_2 = (
    df_claims
    .filter((F.col("claim_type") != "theft") & (F.col("policy_id") <= 18))
    .select("claim_type", "approved_amount", F.lit("claim_2").alias("record_type"))
)

result = (
    claim_side
    .union(claim_side_2)
    .orderBy("claim_type", "record_type")
)
