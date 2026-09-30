from pyspark.sql import functions as F

holder_rows = (
    df_policyholders
    .filter((F.col("joined_on").isNotNull()) & (F.col("segment") == "SME"))
    .select(F.col("region").alias("name"), F.col("id").alias("qty"))
)

claim_rows = df_claims.select(F.col("claim_type").alias("name"), F.col("id").alias("qty"))

result = (
    holder_rows
    .union(claim_rows)
    .orderBy("qty")
)
