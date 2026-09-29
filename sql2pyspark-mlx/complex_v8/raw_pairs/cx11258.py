from pyspark.sql import functions as F

adjuster_rows = df_adjusters.select("level", "id", F.lit("adjuster").alias("src"))

claim_side = (
    df_claims
    .filter((F.col("policy_id") >= 26) & (F.col("adjuster_id").isNotNull()))
    .select("adjuster_id", "policy_id", F.lit("claim").alias("src"))
)

claim_side_2 = (
    df_claims
    .filter(F.col("adjuster_id") <= 7)
    .select("policy_id", "adjuster_id", F.lit("claim").alias("src"))
)

unioned = (
    adjuster_rows
    .union(claim_side)
    .union(claim_side_2)
)

result = (
    unioned
    .groupBy("src")
    .agg(F.count("*").alias("n_rows"), F.sum("id").alias("total_id"))
    .orderBy("src")
)
