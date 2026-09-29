from pyspark.sql import functions as F

base = (
    df_tickets
    .filter(F.col("closed_at") < F.lit("2025-02-21 07:00:00").cast("timestamp"))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "channel",
        "csat",
        "tags",
        F.when(F.col("channel").isin("chat", "phone"), "group_a").when(F.col("channel") == "web", "group_b").otherwise("other").alias("channel_group"),
    )
)

line_level = (
    base.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.channel",
        "s.csat",
        "s.tags",
        F.col("t.id").alias("event_id"),
        "t.event_type",
        "t.event_at",
        F.col("t.actor").alias("event_actor"),
    )
)

scored = (
    line_level
    .filter(F.col("event_type").isin("reopened", "created", "replied"))
    .select(
        "id",
        "agent_id",
        "csat",
        "event_id",
        "event_type",
        "event_at",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.when(F.col("csat") >= 4, "large").when(F.col("csat") >= 3, "medium").otherwise("small").alias("csat_tier"),
    )
)

result = (
    scored
    .cube("agent_id", "event_id", "csat")
    .agg(
        F.max("event_at").alias("last_event_at"),
        F.count(F.when(F.col("event_type") != "reopened", 1)).alias("n_event_type_reopened"),
        F.round(F.sum("days_since_event_at") / F.nullif(F.sum("csat"), F.lit(0)), 3).alias("days_since_event_at_to_csat_ratio"),
    )
)
