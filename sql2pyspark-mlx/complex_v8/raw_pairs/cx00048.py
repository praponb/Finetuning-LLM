from pyspark.sql import functions as F

claim_rows = (
    df_claims
    .filter(F.col("policy_id") >= 10)
    .select(
        F.col("id").alias("ref_id"),
        F.col("claim_type").alias("label"),
        F.lit("claim").alias("source"),
    )
)

claim_rows_2 = (
    df_claims
    .filter((F.col("claimed_amount") < 38909.4) & (F.col("status") != "OPEN"))
    .select(
        F.col("policy_id").alias("ref_id"),
        F.col("status").alias("label"),
        F.lit("claim_2").alias("source"),
    )
)

result = (
    claim_rows
    .unionByName(claim_rows_2)
    .distinct()
)
