from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel_agent_id_by_closed_a = Window.partitionBy("channel", "agent_id").orderBy("closed_at", "id", "event_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_channel_agent_id_by_closed_a_2 = Window.partitionBy("channel", "agent_id").orderBy("closed_at", "id", "event_id").rowsBetween(-2, -1)
w_agent_id_event_actor = Window.partitionBy("agent_id", "event_actor")

cleaned = (
    df_tickets
    .filter(F.size(F.col("tags")) >= 1)
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "tags",
        F.least(F.col("csat"), F.col("requester_id")).alias("least_csat_requester_id"),
        F.abs(F.col("csat") - 3).alias("csat_distance"),
    )
)

detail_rows = (
    cleaned.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.channel",
        "s.csat_distance",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        "t.event_at",
        "t.event_type",
    )
)

with_trends = (
    detail_rows
    .select(
        "id",
        "closed_at",
        "event_id",
        "event_type",
        F.nth_value("closed_at", 2).over(w_channel_agent_id_by_closed_a).alias("second_closed_at"),
        F.round(F.avg("csat_distance").over(w_channel_agent_id_by_closed_a_2), 2).alias("csat_distance_avg_2_prior"),
        F.size(F.collect_set("event_type").over(w_agent_id_event_actor)).alias("distinct_event_type_in_agent_id_event_actor"),
    )
)

exploded_detail = (
    with_trends.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.closed_at",
        "s.event_id",
        "s.event_type",
        "s.second_closed_at",
        "s.csat_distance_avg_2_prior",
        "s.distinct_event_type_in_agent_id_event_actor",
        F.col("t.id").alias("event_id_2"),
        "t.event_at",
        F.col("t.event_type").alias("event_event_type"),
    )
)

qualified = (
    exploded_detail.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.actor") == "system") & (F.col("t.event_at") < F.lit("2025-02-18 10:00:00").cast("timestamp")), "left_anti")
)

with_activity = (
    qualified.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (~F.col("t.actor").isin("system", "agent")) & (F.col("t.event_at") >= F.lit("2025-01-25 09:00:00").cast("timestamp")), "left_semi")
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count(F.when(F.col("event_type").isin("replied", "created", "reopened"), 1)).alias("event_event_type_count"),
    )
)

summarized_rows = (
    with_activity.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_event_type_count"), F.lit(0)).alias("event_event_type_count"),
    )
)

result = (
    summarized_rows
    .filter(F.col("event_event_type").isNotNull())
    .select(
        "id",
        "closed_at",
        "event_id",
        "event_event_type",
        F.dayofweek(F.col("closed_at")).alias("closed_at_dayofweek"),
        F.concat(F.col("event_type"), F.lit("#"), F.col("event_event_type_count").cast("string")).alias("event_type_ref"),
    )
    .orderBy(F.col("closed_at").desc(), F.col("id").asc_nulls_last())
)
