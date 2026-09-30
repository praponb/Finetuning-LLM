from pyspark.sql import functions as F

filtered = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "tags",
    )
)

combined_details = (
    filtered.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.upper(F.trim(F.col("su.agent_name"))).like("H%"))
    .select(
        "s.id",
        "s.agent_id",
        F.col("su.tier").alias("agent_tier"),
        "su.agent_name",
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.min("event_at").alias("first_event_at"),
        F.count("*").alias("event_count"),
        F.min("event_at").alias("first_event_at_2"),
    )
)

with_counts = (
    combined_details.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "t.first_event_at",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        "t.first_event_at_2",
    )
)

grouped = (
    with_counts
    .groupBy("event_count")
    .agg(
        F.array_sort(F.collect_set("agent_name")).alias("agent_name_list"),
        F.count("*").alias("row_count"),
        F.sum(F.when(F.upper(F.trim(F.col("agent_name"))).like("F%"), 1).otherwise(0)).alias("n_upper_trim_agent_name_like_f"),
        F.countDistinct("agent_id").alias("distinct_agent_ids"),
        F.round(F.stddev("event_count"), 2).alias("stddev_event_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.avg("event_count").alias("avg_event_count_check"),
    )
    .filter(F.col("avg_event_count_check") > 3)
    .drop("avg_event_count_check")
)

result = (
    grouped
    .select(
        "event_count",
        "agent_name_list",
        "row_count",
        "n_upper_trim_agent_name_like_f",
        "distinct_agent_ids",
        (F.col("distinct_ids") + F.col("row_count")).alias("distinct_ids_plus_row_count"),
    )
    .orderBy(F.col("event_count").asc(), F.col("row_count").desc())
)
