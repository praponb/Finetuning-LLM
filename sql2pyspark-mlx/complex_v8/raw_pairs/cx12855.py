from pyspark.sql import functions as F

claim_rows = df_claims.select("policy_id", F.col("reported_at").alias("happened_at"), "status")

claim_rows_2 = df_claims.select("policy_id", "reported_at", "status")

result = claim_rows.union(claim_rows_2)
