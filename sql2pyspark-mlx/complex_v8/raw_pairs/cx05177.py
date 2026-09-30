from pyspark.sql import functions as F

valid_claims = (
    df_claims
    .filter((F.col("claimed_amount") >= 19609.8) & (F.col("claim_date").between(F.lit("2024-04-27").cast("date"), F.lit("2025-10-15").cast("date"))))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at",
    )
)

claim_rows = (
    valid_claims
    .select(
        F.lit("claim").alias("source"),
        "claim_type",
        F.col("reported_at").alias("event_time"),
        F.col("adjuster_id").alias("entity_id"),
    )
)

claim_side = (
    df_claims
    .filter(F.col("status").isin("REJECTED", "IN_REVIEW"))
    .select(
        F.lit("claim_2").alias("source"),
        "claim_type",
        F.col("reported_at").alias("event_time"),
        F.col("id").alias("entity_id"),
    )
)

claim_side_2 = (
    df_claims
    .filter((F.col("adjuster_id").isNotNull()) & (F.col("claimed_amount") <= 30115.9))
    .select(
        F.lit("claim_3").alias("source"),
        F.col("status").alias("claim_type"),
        F.col("reported_at").alias("event_time"),
        F.col("id").alias("entity_id"),
    )
)

all_events = (
    claim_rows
    .unionByName(claim_side)
    .unionByName(claim_side_2)
)

result = (
    all_events
    .groupBy("source")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("entity_id").alias("total_entity_id"),
        F.max("event_time").alias("latest_event_time"),
    )
    .orderBy("source")
)
