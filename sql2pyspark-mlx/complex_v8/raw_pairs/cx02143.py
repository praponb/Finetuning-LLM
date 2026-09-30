from pyspark.sql import functions as F

claim_side = df_claims.select(F.col("id").alias("qty"), F.col("reported_at").alias("happened_at"))

claim_side_2 = df_claims.select("policy_id", "reported_at")

result = claim_side.union(claim_side_2)
