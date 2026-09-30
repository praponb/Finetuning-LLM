from pyspark.sql import functions as F

claim_rows = df_claims.select("policy_id", "status")

holder_rows = (
    df_policyholders
    .filter((F.col("region").isin("North", "South", "Central")) & (F.col("segment") == "VIP"))
    .select("id", "region")
)

result = (
    claim_rows
    .exceptAll(holder_rows)
    .orderBy("policy_id", "status")
)
