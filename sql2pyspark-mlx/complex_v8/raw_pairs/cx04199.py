from pyspark.sql import functions as F

claim_rows = (
    df_claims
    .select(
        F.lit("claim").alias("record_type"),
        "adjuster_id",
        F.col("claim_date").alias("day"),
        F.col("reported_at").alias("event_time"),
    )
)

claim_side = (
    df_claims
    .select(
        F.lit("claim_2").alias("record_type"),
        F.col("policy_id").alias("adjuster_id"),
        F.col("claim_date").alias("day"),
        F.col("reported_at").alias("event_time"),
    )
)

claim_rows_2 = (
    df_claims
    .select(
        F.lit("claim_3").alias("record_type"),
        F.col("id").alias("adjuster_id"),
        F.col("claim_date").alias("day"),
        F.col("reported_at").alias("event_time"),
    )
)

stacked = (
    claim_rows
    .unionByName(claim_side)
    .unionByName(claim_rows_2)
)

result = (
    stacked
    .groupBy("record_type")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("adjuster_id").alias("total_adjuster_id"),
        F.max("day").alias("latest_day"),
    )
    .orderBy("record_type")
)
