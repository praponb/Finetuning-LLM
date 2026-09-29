from pyspark.sql import functions as F

cleaned = (
    df_claims
    .filter(~F.col("claim_type").isin("fire", "collision"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "approved_amount",
        "status",
    )
)

screened = (
    cleaned
    .crossJoin(cleaned.agg(F.avg("approved_amount").alias("overall_avg_approved_amount")))
    .filter(F.col("approved_amount") > F.col("overall_avg_approved_amount"))
    .drop("overall_avg_approved_amount")
)

result = (
    screened
    .rollup("claim_type", "adjuster_id", "status")
    .agg(
        F.count("*").alias("n_rows"),
        F.round(F.avg("approved_amount"), 2).alias("avg_approved_amount"),
        F.array_sort(F.collect_set("status")).alias("status_list"),
    )
)
