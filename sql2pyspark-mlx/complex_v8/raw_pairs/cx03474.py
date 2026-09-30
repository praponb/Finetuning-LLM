from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("event_type", "ticket_id").orderBy(F.col("event_at").desc(), F.col("id").desc())
w_event_at_to_date_by_event_at = Window.partitionBy(F.to_date("event_at")).orderBy(F.col("event_at").desc_nulls_first())
w_event_type_by_event_at_runni = Window.partitionBy("event_type").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_at_to_date_by_event_at_2 = Window.partitionBy(F.to_date("event_at")).orderBy(F.col("event_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_events = (
    df_ticket_events
    .filter((F.hour(F.col("event_at")).between(9, 13)) & (F.col("event_type").isin("reopened", "replied")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

latest_per_key = (
    filtered_events
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

windowed = (
    latest_per_key
    .select(
        "*",
        F.round(F.percent_rank().over(w_event_at_to_date_by_event_at), 3).alias("event_at_percent_rank"),
        F.last("event_at", ignorenulls=True).over(w_event_type_by_event_at_runni).alias("last_known_event_at"),
        F.first("event_at", ignorenulls=True).over(w_event_at_to_date_by_event_at_2).alias("first_known_event_at"),
    )
)

enriched = (
    windowed.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.event_at_percent_rank",
        "s.last_known_event_at",
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("su.team").alias("agent_team"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count("*").alias("event_count"),
        F.max("event_at").alias("last_event_at"),
        F.min("event_at").alias("first_event_at"),
    )
)

profiled = (
    enriched.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        "t.last_event_at",
        "t.first_event_at",
    )
)

exploded_detail = (
    profiled.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.actor",
        "s.last_known_event_at",
        "s.ticket_requester_id",
        "s.agent_team",
        "s.event_count",
        "s.last_event_at",
        "s.first_event_at",
        F.col("t.id").alias("event_id"),
        F.col("t.event_type").alias("event_event_type"),
        F.col("t.actor").alias("event_actor"),
    )
)

grouped = (
    exploded_detail
    .filter(F.col("event_actor").isin("system", "agent"))
    .groupBy(
        F.to_date("event_at").alias("event_at_day"),
        F.year("last_known_event_at").alias("last_known_event_at_year"),
        "ticket_id",
    )
    .agg(
        F.min("first_event_at").alias("first_first_event_at"),
        F.array_sort(F.collect_set("agent_team")).alias("agent_team_list"),
        F.sum(F.when(~F.col("event_actor").isin("system", "customer"), 1).otherwise(0)).alias("n_event_actor_not_in_system_cu"),
        F.avg("event_count").alias("avg_event_count_check"),
    )
    .filter(F.col("avg_event_count_check") > 1)
    .drop("avg_event_count_check")
)

result = (
    grouped
    .select(
        "event_at_day",
        "last_known_event_at_year",
        "first_first_event_at",
        "agent_team_list",
        "n_event_actor_not_in_system_cu",
        F.quarter(F.col("event_at_day")).alias("event_at_day_quarter"),
        F.abs(F.col("last_known_event_at_year") - 2025).alias("last_known_event_at_year_distance"),
    )
)
