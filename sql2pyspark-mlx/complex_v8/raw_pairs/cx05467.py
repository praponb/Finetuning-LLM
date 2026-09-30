from pyspark.sql import functions as F

claim_side = (
    df_claims
    .filter((F.col("adjuster_id") >= 4) & (F.col("claimed_amount").between(6745.1, 46415.5)))
    .select("status", F.col("adjuster_id").alias("qty"), "claim_type")
)

holder_side = df_policyholders.select("holder_name", "id", "segment")

result = (
    claim_side
    .union(holder_side)
    .orderBy(F.col("status").desc())
)
