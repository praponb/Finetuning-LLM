from pyspark.sql import functions as F

claim_side = (
    df_claims
    .filter(F.col("status") != "OPEN")
    .select(
        F.col("adjuster_id").alias("qty"),
        "reported_at",
        "claimed_amount",
        F.lit("claim").alias("record_type"),
    )
)

claim_side_2 = (
    df_claims
    .select(
        F.col("id").alias("qty"),
        "reported_at",
        "claimed_amount",
        F.lit("claim_2").alias("record_type"),
    )
)

combined = claim_side.unionByName(claim_side_2)

result = (
    combined
    .groupBy("record_type")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("qty").alias("total_qty"),
        F.max("reported_at").alias("latest_reported_at"),
    )
    .orderBy("record_type")
)
