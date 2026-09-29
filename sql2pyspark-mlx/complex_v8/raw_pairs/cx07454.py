from pyspark.sql import functions as F

claim_side = (
    df_claims
    .filter((F.col("claim_date") >= F.lit("2024-06-13").cast("date")) & (F.col("policy_id") >= 24))
    .select(F.col("adjuster_id").alias("ref_id"), "status")
)

claim_side_2 = df_claims.select("policy_id", "claim_type")

policy_side = (
    df_policies
    .filter((F.col("holder_id") <= 2) & (F.col("start_date").isNotNull()))
    .select("holder_id", "status")
)

combined = (
    claim_side
    .union(claim_side_2)
    .union(policy_side)
)

result = (
    combined
    .groupBy("status")
    .agg(F.count("*").alias("n_rows"), F.sum("ref_id").alias("total_ref_id"))
    .orderBy("status")
)
