from pyspark.sql import functions as F

base = (
    df_tickets
    .filter((F.col("opened_at") >= F.lit("2025-02-06 11:00:00").cast("timestamp")) | (F.col("csat") < 4))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        (F.floor(F.col("requester_id") / 5) * 5).alias("requester_id_bucket"),
    )
)

with_activity = (
    base.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.hour(F.col("t.event_at")).between(9, 15)) & (F.col("t.event_type").isin("replied", "reopened", "resolved")), "left_semi")
    .filter(F.col("agent_id") >= 4)
)

scored = (
    with_activity
    .filter(F.col("closed_at") >= F.lit("2025-02-08 15:00:00").cast("timestamp"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "requester_id_bucket",
        F.abs(F.col("requester_id_bucket") - 15).alias("requester_id_bucket_distance"),
        (F.floor(F.col("requester_id") / 5) * 5).alias("requester_id_bucket_2"),
        (F.col("requester_id_bucket") + F.col("requester_id")).alias("requester_id_bucket_plus_requester_id"),
    )
)

result = (
    scored
    .filter(F.col("requester_id_bucket_plus_requester_id") <= 30)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "requester_id_bucket",
        "requester_id_bucket_distance",
        "requester_id_bucket_2",
        "requester_id_bucket_plus_requester_id",
    )
    .orderBy("id")
)
