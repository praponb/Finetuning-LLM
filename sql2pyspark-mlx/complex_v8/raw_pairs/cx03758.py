from pyspark.sql import functions as F

filtered_events = (
    df_ticket_events
    .filter(F.col("actor").isin("agent", "customer"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.upper(F.trim(F.col("actor"))).alias("actor_clean"),
        F.length(F.trim(F.col("actor"))).alias("actor_len"),
    )
)

with_activity = (
    filtered_events.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.actor").isin("agent", "system")) & (~F.col("t.event_type").isin("reopened", "created")), "left_semi")
)

annotated = with_activity.withColumn("actor_len_bucket", F.floor(F.col("actor_len") / 2) * 2)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.max("event_at").alias("last_event_at"),
        F.count(F.when(F.col("actor").isNotNull(), 1)).alias("event_actor_count"),
    )
)

summarized_rows = (
    annotated.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        "t.last_event_at",
        F.coalesce(F.col("t.event_actor_count"), F.lit(0)).alias("event_actor_count"),
    )
)

result = (
    summarized_rows
    .select(
        "id",
        F.col("actor_len_bucket").alias("actor_len_bucket_v"),
        F.col("actor_len").alias("m_actor_len"),
        F.col("event_actor_count").alias("m_event_actor_count"),
    )
    .unpivot(
        ids=["id"],
        values=["actor_len_bucket_v", "m_actor_len", "m_event_actor_count"],
        variableColumnName="field",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
