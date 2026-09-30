from pyspark.sql import functions as F

adjuster_rows = (
    df_adjusters
    .filter(F.col("team") == "Property")
    .select("id", "level")
)

claim_rows = df_claims.select(F.col("adjuster_id").alias("id"), F.col("policy_id").alias("level"))

result = (
    adjuster_rows
    .unionByName(claim_rows)
    .distinct()
    .orderBy("level")
)
