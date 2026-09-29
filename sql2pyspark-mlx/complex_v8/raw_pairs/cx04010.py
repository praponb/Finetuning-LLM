from pyspark.sql import functions as F

claim_side = (
    df_claims
    .select(
        F.col("id").alias("key_id"),
        F.col("adjuster_id").alias("key_id_2"),
        "claim_date",
    )
)

claim_side_2 = df_claims.select("adjuster_id", "id", "claim_date")

result = (
    claim_side
    .union(claim_side_2)
    .orderBy("key_id", "claim_date")
)
