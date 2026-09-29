from pyspark.sql import functions as F

valid_events = (
    df_ticket_events
    .filter((F.col("actor") == "customer") & (F.col("ticket_id").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.length(F.trim(F.col("event_type"))).alias("event_type_len"),
    )
)

with_children = (
    valid_events.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type_len",
        F.col("t.id").alias("event_id"),
        F.col("t.event_at").alias("event_event_at"),
        F.col("t.actor").alias("event_actor"),
        F.col("t.event_type").alias("event_event_type"),
    )
)

tagged = (
    with_children
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "*",
        F.when(F.col("event_type_len") >= 9, "large").when(F.col("event_type_len") >= 5, "medium").otherwise("small").alias("event_type_len_tier"),
        (F.floor(F.col("event_type_len") / 2) * 2).alias("event_type_len_bucket"),
        F.when(F.col("event_event_type").isin("reopened", "resolved"), "group_a").when(F.col("event_event_type") == "assigned", "group_b").otherwise("other").alias("event_event_type_group"),
    )
)

result = (
    tagged
    .groupBy("ticket_id")
    .pivot("event_type_len_tier", ["small", "medium", "large"])
    .agg(F.sum("event_type_len"))
)
