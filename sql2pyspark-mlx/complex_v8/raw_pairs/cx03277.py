from pyspark.sql import functions as F

base_events = df_ticket_events.filter(F.col("ticket_id").isNotNull())

scored = (
    base_events
    .filter(F.col("ticket_id") >= 19)
    .select(
        "id",
        "event_type",
        "actor",
        F.length(F.trim(F.col("event_type"))).alias("event_type_len"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("event_at")).alias("days_since_event_at"),
    )
)

labeled = (
    scored
    .filter(F.col("actor").isNotNull())
    .select(
        "*",
        F.abs(F.col("event_type_len") - 10).alias("event_type_len_distance"),
        F.when(F.col("event_type").isin("created", "resolved"), "group_a").when(F.col("event_type") == "reopened", "group_b").otherwise("other").alias("event_type_group"),
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
    )
)

result = (
    labeled
    .filter(F.col("actor") == "agent")
    .select(
        "id",
        "actor",
        "event_type_len",
        "event_type_len_distance",
        "event_type_group",
        "actor_slug",
        F.when(F.col("event_type_len_distance") >= 9, "A").when(F.col("event_type_len_distance") >= 5, "B").otherwise("C").alias("event_type_len_distance_tier"),
    )
    .orderBy(F.col("event_type_group").desc(), "event_type_len")
)
