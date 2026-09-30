from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel_agent_team = Window.partitionBy("channel", "agent_team")
w_channel_closed_at_month_by_l = Window.partitionBy("channel", F.month("closed_at")).orderBy(F.col("least_csat_minus_requester_id_requester_id_plus_").desc_nulls_first(), "id")
w_channel_closed_at_month_by_l_2 = Window.partitionBy("channel", F.month("closed_at")).orderBy(F.col("least_csat_minus_requester_id_requester_id_plus_").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_channel_closed_at_month_by_l_3 = Window.partitionBy("channel", F.month("closed_at")).orderBy(F.col("least_csat_minus_requester_id_requester_id_plus_").desc_nulls_first())

cleaned_tickets = (
    df_tickets
    .filter((~F.col("channel").isin("chat", "web")) & (F.col("csat") > 2))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        (F.col("csat") - F.col("requester_id")).alias("csat_minus_requester_id"),
        (F.col("requester_id") + F.col("csat")).alias("requester_id_plus_csat"),
    )
)

enriched = (
    cleaned_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.channel",
        "s.csat_minus_requester_id",
        "s.requester_id_plus_csat",
        F.col("su.team").alias("agent_team"),
    )
)

enriched_2 = (
    enriched
    .select(
        "id",
        "closed_at",
        "channel",
        "csat_minus_requester_id",
        "requester_id_plus_csat",
        "agent_team",
        (F.col("closed_at") + F.expr("INTERVAL 7 DAYS")).alias("closed_at_plus_7d"),
        F.least(F.col("csat_minus_requester_id"), F.col("requester_id_plus_csat")).alias("least_csat_minus_requester_id_requester_id_plus_"),
        F.date_format(F.col("closed_at"), "yyyy-MM-dd HH:00").alias("closed_at_label"),
    )
)

channel_stats = (
    enriched_2
    .groupBy("channel")
    .agg(F.avg("csat_minus_requester_id").alias("avg_csat_minus_requester_id"))
)

above_average = (
    enriched_2.alias("s")
    .join(channel_stats.alias("g"), F.col("g.channel") == F.col("s.channel"), "inner")
    .filter(F.col("s.csat_minus_requester_id") > F.col("g.avg_csat_minus_requester_id"))
    .select(
        "s.*",
        "g.avg_csat_minus_requester_id",
        F.round(F.col("s.csat_minus_requester_id") - F.col("g.avg_csat_minus_requester_id"), 2).alias("csat_minus_requester_id_minus_avg"),
    )
)

with_windows = (
    above_average
    .select(
        "*",
        F.min("requester_id_plus_csat").over(w_channel_agent_team).alias("channel_agent_team_min_requester_id_plus_csat"),
        F.lead("csat_minus_requester_id").over(w_channel_closed_at_month_by_l).alias("next_csat_minus_requester_id"),
        F.ntile(3).over(w_channel_closed_at_month_by_l).alias("least_csat_minus_requester_id_requester_id_plus__2"),
        F.nth_value("closed_at_plus_7d", 2).over(w_channel_closed_at_month_by_l_2).alias("second_closed_at_plus_7d"),
        F.round(F.percent_rank().over(w_channel_closed_at_month_by_l_3), 3).alias("least_csat_minus_requester_id_requester_id_plus__3"),
        F.round((F.unix_timestamp("closed_at") - F.unix_timestamp(F.lag("closed_at").over(w_channel_closed_at_month_by_l))) / 60, 1).alias("minutes_since_prev_closed_at"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("event_type") != "assigned", 1)).alias("event_event_type_count"),
        F.count("*").alias("event_count_2"),
    )
)

summarized_rows = (
    with_windows.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        F.coalesce(F.col("t.event_event_type_count"), F.lit(0)).alias("event_event_type_count"),
        F.coalesce(F.col("t.event_count_2"), F.lit(0)).alias("event_count_2"),
    )
)

result = (
    summarized_rows
    .filter(F.col("minutes_since_prev_closed_at") < 4500.0)
    .select(
        "id",
        "closed_at",
        "channel",
        "csat_minus_requester_id",
        "requester_id_plus_csat",
        "agent_team",
        "closed_at_plus_7d",
        "least_csat_minus_requester_id_requester_id_plus_",
        "closed_at_label",
        "csat_minus_requester_id_minus_avg",
        "channel_agent_team_min_requester_id_plus_csat",
        "next_csat_minus_requester_id",
        "least_csat_minus_requester_id_requester_id_plus__2",
        "minutes_since_prev_closed_at",
        "event_event_type_count",
        "event_count_2",
        F.length(F.trim(F.col("agent_team"))).alias("agent_team_len"),
    )
)
