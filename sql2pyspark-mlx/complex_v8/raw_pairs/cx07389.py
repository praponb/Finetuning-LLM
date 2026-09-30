from pyspark.sql import functions as F

claim_side = df_claims.select("status", F.col("id").alias("qty"), F.lit("claim").alias("source"))

claim_side_2 = df_claims.select("status", "id", F.lit("claim_2").alias("source"))

result = (
    claim_side
    .union(claim_side_2)
    .orderBy("source", "qty")
)
