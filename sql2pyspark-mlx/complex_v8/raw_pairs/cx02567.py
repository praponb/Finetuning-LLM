from pyspark.sql import functions as F

scoped_events = (
    df_ticket_events
    .filter((F.col("ticket_id").isNotNull()) & (F.col("event_type") == "created"))
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.min("event_at").alias("first_event_at"),
        F.count("*").alias("event_count"),
    )
)

summarized_rows = (
    scoped_events.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        "t.first_event_at",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
    )
)

without_matches = (
    summarized_rows.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.priority") != "P4") & (F.col("t.channel") == "web"), "left_semi")
)

enriched = without_matches.withColumn("actor_clean", F.lower(F.trim(F.col("actor"))))

labeled = (
    enriched
    .filter(F.col("event_at") < F.lit("2025-03-02 09:00:00").cast("timestamp"))
    .select(
        "id",
        "event_at",
        "event_type",
        "actor",
        "first_event_at",
        "event_count",
        "actor_clean",
        F.round((F.unix_timestamp(F.col("event_at")) - F.unix_timestamp(F.col("first_event_at"))) / 60, 1).alias("minutes_first_event_at_to_event_at"),
        F.date_format(F.col("first_event_at"), "yyyy-MM-dd").alias("first_event_at_label"),
        F.initcap(F.trim(F.col("actor"))).alias("actor_clean_2"),
    )
)

result = (
    labeled
    .select(
        "id",
        "event_at",
        "event_type",
        "first_event_at",
        "event_count",
        "actor_clean",
        "minutes_first_event_at_to_event_at",
        "first_event_at_label",
        "actor_clean_2",
        F.when(F.col("event_type").isin("created", "escalated"), "group_a").when(F.col("event_type") == "reopened", "group_b").otherwise("other").alias("event_type_group"),
        F.coalesce(F.col("event_count"), F.lit(0)).alias("event_count_filled"),
    )
    .orderBy(F.col("id").asc_nulls_last())
)
