from pyspark.sql import functions as F

scoped = (
    df_ticket_events
    .filter(F.col("event_at") >= F.lit("2025-01-25 08:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_trunc("quarter", F.col("event_at")).alias("event_at_quarter"),
    )
)

tagged = (
    scoped
    .filter(F.col("event_at_quarter") >= F.lit("2025-01-25 09:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "event_at_quarter",
        F.concat_ws(" / ", F.col("actor"), F.col("event_type")).alias("actor_event_type_label"),
        F.hour(F.col("event_at_quarter")).alias("event_at_quarter_hour"),
        F.substring(F.trim(F.col("actor")), 1, 3).alias("actor_prefix"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count(F.when(F.col("event_type") == "created", 1)).alias("event_event_type_count"),
        F.min("event_at").alias("first_event_at"),
    )
)

summarized_rows = (
    tagged.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_event_type_count"), F.lit(0)).alias("event_event_type_count"),
        "t.first_event_at",
    )
)

tagged_2 = (
    summarized_rows
    .select(
        "*",
        F.date_format(F.col("first_event_at"), "yyyy-MM-dd HH:00").alias("first_event_at_label"),
        F.concat_ws(" / ", F.col("event_type"), F.col("actor_event_type_label")).alias("event_type_actor_event_type_label_label"),
    )
)

result = (
    tagged_2
    .groupingSets(
        [["event_type", "ticket_id"], ["event_type"], ["ticket_id"]],
        "event_type", "ticket_id",
    )
    .agg(
        F.grouping("event_type").alias("is_total_event_type"),
        F.round(F.sum("event_event_type_count") / F.nullif(F.sum("event_at_quarter_hour"), F.lit(0)), 3).alias("event_event_type_count_to_event_at_quarter_hour_"),
    )
    .orderBy(
        F.col("event_type").asc_nulls_last(),
        F.col("ticket_id").asc_nulls_last(),
    )
)
