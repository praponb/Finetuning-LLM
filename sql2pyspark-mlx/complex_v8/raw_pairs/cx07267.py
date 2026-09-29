from pyspark.sql import functions as F

filtered_claims = (
    df_claims
    .filter((~F.col("status").isin("APPROVED", "IN_REVIEW")) & (F.col("approved_amount") < 14668.0))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
    )
)

tagged = (
    filtered_claims
    .select(
        "*",
        F.substring(F.trim(F.col("status")), 1, 4).alias("status_prefix"),
        F.date_format(F.col("reported_at"), "MMM yyyy").alias("reported_at_label"),
        F.substring(F.trim(F.col("status")), 1, 3).alias("status_prefix_2"),
    )
)

result = (
    tagged
    .groupBy("policy_id")
    .pivot("status", ["OPEN", "REJECTED"])
    .agg(F.min("approved_amount"))
    .orderBy("policy_id")
)
