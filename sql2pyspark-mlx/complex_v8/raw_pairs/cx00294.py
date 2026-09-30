from pyspark.sql import functions as F

policy_side = (
    df_policies
    .filter(F.col("product") != "AUTO")
    .select("id", F.col("start_date").alias("event_date"), "holder_id")
)

claim_rows = (
    df_claims
    .filter((F.col("approved_amount") > 9389.3) & (F.col("status") == "IN_REVIEW"))
    .select(
        F.col("adjuster_id").alias("id"),
        F.col("claim_date").alias("event_date"),
        F.col("id").alias("holder_id"),
    )
)

stacked = policy_side.unionByName(claim_rows)

result = (
    stacked
    .groupBy("id")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("holder_id").alias("total_holder_id"),
        F.max("event_date").alias("latest_event_date"),
    )
    .orderBy("id")
)
