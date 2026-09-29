from pyspark.sql import functions as F

claim_rows = (
    df_claims
    .select(
        "claim_type",
        F.col("reported_at").alias("event_time"),
        F.lit("claim").alias("record_type"),
    )
)

claim_rows_2 = (
    df_claims
    .filter((F.col("claimed_amount") >= 14019.8) & (F.col("claim_type").isin("theft", "collision", "fire")))
    .select("status", "reported_at", F.lit("claim_2").alias("record_type"))
)

combined = (
    claim_rows
    .union(claim_rows_2)
    .distinct()
)

result = (
    combined
    .groupBy("record_type")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("event_time").alias("latest_event_time"),
    )
    .orderBy("record_type")
)
