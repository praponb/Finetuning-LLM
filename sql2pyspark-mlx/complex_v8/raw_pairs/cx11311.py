from pyspark.sql import functions as F

claim_side = df_claims.select(F.col("policy_id").alias("entity_id"))

adjuster_side = df_adjusters.select("id")

result = (
    claim_side
    .subtract(adjuster_side)
    .orderBy(F.col("entity_id").desc())
)
