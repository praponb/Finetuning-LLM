from pyspark.sql import functions as F

prepared_tickets = (
    df_tickets
    .filter((F.col("priority").isin("P4", "P1")) | (F.col("opened_at") >= F.lit("2025-01-13 12:00:00").cast("timestamp")))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "channel",
        "csat",
        "tags",
        F.lower(F.trim(F.col("priority"))).alias("priority_clean"),
    )
)

derived = (
    prepared_tickets
    .filter(~F.array_contains(F.col("tags"), "outage"))
    .withColumn("days_since_closed_at", F.datediff(F.lit("2025-12-31").cast("date"), F.col("closed_at")))
)

scored = (
    derived
    .select(
        "id",
        "requester_id",
        "closed_at",
        "channel",
        "tags",
        "priority_clean",
        "days_since_closed_at",
        F.lower(F.trim(F.col("channel"))).alias("channel_clean"),
        F.lower(F.regexp_replace(F.trim(F.col("priority_clean")), " ", "_")).alias("priority_clean_slug"),
        (F.floor(F.col("requester_id") / 5) * 5).alias("requester_id_bucket"),
    )
)

result = (
    scored
    .select(
        "id",
        "requester_id",
        "channel",
        "channel_clean",
        "requester_id_bucket",
        F.coalesce(F.col("channel"), F.lit("UNKNOWN")).alias("channel_or_unknown"),
    )
)
