from pyspark.sql import functions as F

claim_side = (
    df_claims
    .select(
        F.col("policy_id").alias("qty"),
        F.col("reported_at").alias("happened_at"),
    )
)

claim_side_2 = df_claims.select(F.col("id").alias("qty"), F.col("reported_at").alias("happened_at"))

result = (
    claim_side
    .unionByName(claim_side_2)
    .orderBy("happened_at", "qty")
)
