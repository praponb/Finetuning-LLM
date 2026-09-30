from pyspark.sql import functions as F

valid_claims = df_claims.filter(F.col("adjuster_id").isNotNull())

claim_rows = valid_claims.select(F.col("id").alias("key_id"), F.col("claim_date").alias("event_date"))

policy_side = (
    df_policies
    .filter((F.col("holder_id") >= 11) & (F.col("start_date").isNotNull()))
    .select(
        F.col("holder_id").alias("key_id"),
        F.col("end_date").alias("event_date"),
    )
)

result = claim_rows.unionByName(policy_side)
