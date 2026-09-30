from pyspark.sql import functions as F

policy_side = (
    df_policies
    .select(
        F.lit("policy").alias("source"),
        F.col("holder_id").alias("qty"),
        "end_date",
        F.col("product").alias("category"),
    )
)

claim_rows = (
    df_claims
    .filter((F.col("adjuster_id") <= 3) & (F.col("claimed_amount") <= 32878.5))
    .select(
        F.lit("claim").alias("source"),
        F.col("policy_id").alias("qty"),
        F.col("claim_date").alias("end_date"),
        F.col("status").alias("category"),
    )
)

unioned = policy_side.union(claim_rows)

result = (
    unioned
    .groupBy("source")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("qty").alias("total_qty"),
        F.max("end_date").alias("latest_end_date"),
    )
    .orderBy("source")
)
