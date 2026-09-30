from pyspark.sql import functions as F

claim_rows = df_claims.select("adjuster_id", "policy_id", F.col("status").alias("label"))

policy_rows = (
    df_policies
    .filter((~F.col("product").isin("HEALTH", "LIFE")) & (F.col("holder_id").isNotNull()))
    .select("id", "holder_id", "status")
)

combined = claim_rows.union(policy_rows)

result = (
    combined
    .groupBy("label")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("adjuster_id").alias("total_adjuster_id"),
    )
    .orderBy("label")
)
