from pyspark.sql import functions as F

scoped = (
    df_ticket_events
    .filter(F.col("ticket_id") <= 26)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

tagged = scoped.select("*", F.initcap(F.trim(F.col("actor"))).alias("actor_clean"))

detail_rows = (
    tagged.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.actor",
        "s.actor_clean",
        F.col("t.id").alias("event_id"),
        F.col("t.event_at").alias("event_event_at"),
        F.col("t.actor").alias("event_actor"),
        F.col("t.event_type").alias("event_event_type"),
    )
)

without_matches = (
    detail_rows.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_at") >= F.lit("2025-01-25 08:00:00").cast("timestamp")) & (F.col("t.actor") != "agent"), "left_anti")
)

stats = (
    without_matches
    .groupBy(
        "ticket_id",
        F.date_format("event_event_at", "yyyy-MM").alias("event_event_at_ym"),
    )
    .agg(
        F.array_sort(F.collect_set("actor")).alias("actor_list"),
        F.count("*").alias("cnt"),
        F.array_sort(F.collect_set("event_event_type")).alias("event_event_type_list"),
    )
)

screened = (
    stats
    .crossJoin(stats.agg(F.min("cnt").alias("overall_min_cnt")))
    .filter(F.col("cnt") > F.col("overall_min_cnt"))
    .drop("overall_min_cnt")
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count(F.when(~F.col("event_type").isin("escalated", "reopened"), 1)).alias("event_event_type_count"),
        F.max("event_at").alias("last_event_at"),
        F.count("*").alias("event_count"),
    )
)

with_stats = (
    screened.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_event_type_count"), F.lit(0)).alias("event_event_type_count"),
        "t.last_event_at",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
    )
)

result = (
    with_stats
    .select(
        "ticket_id",
        "event_event_at_ym",
        "actor_list",
        "cnt",
        "event_event_type_list",
        "event_event_type_count",
        "event_count",
        F.substring(F.trim(F.col("event_event_at_ym")), 1, 3).alias("event_event_at_ym_prefix"),
    )
    .orderBy("ticket_id", F.col("cnt").desc_nulls_first())
)
