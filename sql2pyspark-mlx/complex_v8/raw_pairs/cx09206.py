from pyspark.sql import functions as F

valid = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat_ws("|", F.col("event_type"), F.col("actor")).alias("event_type_actor_label"),
        F.dayofweek(F.col("event_at")).alias("event_at_dayofweek"),
        F.when(F.col("actor").isin("customer", "agent"), "group_a").when(F.col("actor") == "system", "group_b").otherwise("other").alias("actor_group"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.max("event_at").alias("last_event_at"))
)

summarized_rows = (
    valid.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select("s.*", "t.last_event_at")
)

event_stats_2 = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.count("*").alias("event_count"))
)

with_stats = (
    summarized_rows.alias("s")
    .join(event_stats_2.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
    )
)

event_stats_3 = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count(F.when(F.col("event_type") == "assigned", 1)).alias("event_event_type_count"),
        F.count(F.when(F.col("event_type") == "reopened", 1)).alias("event_event_type_count_2"),
        F.min("event_at").alias("first_event_at"),
    )
)

summarized_rows_2 = (
    with_stats.alias("s")
    .join(event_stats_3.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_event_type_count"), F.lit(0)).alias("event_event_type_count"),
        F.coalesce(F.col("t.event_event_type_count_2"), F.lit(0)).alias("event_event_type_count_2"),
        "t.first_event_at",
    )
)

result = (
    summarized_rows_2
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type_actor_label",
        "event_at_dayofweek",
        "event_count",
        "event_event_type_count",
        "event_event_type_count_2",
        "first_event_at",
    )
    .orderBy("event_count", "id")
    .limit(20)
)
