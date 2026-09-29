from pyspark.sql import functions as F

claim_side = (
    df_claims
    .filter(F.col("claim_type") == "collision")
    .select(
        F.col("status").alias("label"),
        F.col("reported_at").alias("happened_at"),
    )
)

claim_side_2 = (
    df_claims
    .select(
        F.col("claim_type").alias("label"),
        F.col("reported_at").alias("happened_at"),
    )
)

claim_rows = (
    df_claims
    .select(
        F.col("status").alias("label"),
        F.col("reported_at").alias("happened_at"),
    )
)

result = (
    claim_side
    .unionByName(claim_side_2)
    .unionByName(claim_rows)
    .orderBy("happened_at", "label")
)
