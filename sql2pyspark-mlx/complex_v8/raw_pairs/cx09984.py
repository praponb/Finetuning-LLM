from pyspark.sql import functions as F

claim_side = df_claims.select("policy_id")

adjuster_side = (
    df_adjusters
    .filter((F.col("team") != "Travel") & (F.col("adjuster_name").isNotNull()))
    .select(F.col("level").alias("policy_id"))
)

result = (
    claim_side
    .exceptAll(adjuster_side)
    .orderBy("policy_id")
)
