from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_at_date_month = Window.partitionBy(F.date_trunc("month", "event_at_date"))
w_ticket_csat_by_ticket_csat_p = Window.partitionBy("ticket_csat").orderBy("ticket_csat", "id").rowsBetween(-6, -1)
w_actor_ticket_csat_by_ticket = Window.partitionBy("actor", "ticket_csat").orderBy(F.col("ticket_requester_id").desc(), "id")
w_ticket_csat_by_ticket_csat = Window.partitionBy("ticket_csat").orderBy("ticket_csat", "id")
w_ticket_channel_event_at_date = Window.partitionBy("ticket_channel", F.date_trunc("month", "event_at_date")).orderBy("event_at_plus_15m", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_actor_ticket_csat_by_ticket_2 = Window.partitionBy("actor", "ticket_csat").orderBy(F.col("ticket_requester_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_channel_event_at_date_2 = Window.partitionBy("ticket_channel", F.date_trunc("month", "event_at_date")).orderBy("event_at_plus_15m", "id")
w_event_at_date_month_by_disti = Window.partitionBy(F.month("event_at_date")).orderBy("distinct_agent_team_in_event_at_date_month")
w_event_at_date_month_by_disti_2 = Window.partitionBy(F.month("event_at_date")).orderBy("distinct_agent_team_in_event_at_date_month", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_at_date_month_by_disti_3 = Window.partitionBy(F.month("event_at_date")).orderBy("distinct_agent_team_in_event_at_date_month", "id")
w_ticket_id_ticket_requester_i = Window.partitionBy("ticket_id", "ticket_requester_id_quartile")

scoped_events = (
    df_ticket_events
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.to_date(F.col("event_at")).alias("event_at_date"),
        (F.col("event_at") + F.expr("INTERVAL 15 MINUTES")).alias("event_at_plus_15m"),
    )
)

combined_details_ticket = (
    scoped_events.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.requester_id") > 3), "left")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.actor",
        "s.event_at_date",
        "s.event_at_plus_15m",
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.channel").alias("ticket_channel"),
        F.col("su.tier").alias("agent_tier"),
        F.col("su.team").alias("agent_team"),
    )
)

with_trends = (
    combined_details_ticket
    .select(
        "id",
        "ticket_id",
        "event_at_date",
        "event_at_plus_15m",
        "ticket_csat",
        "ticket_requester_id",
        "ticket_channel",
        "agent_tier",
        "agent_team",
        F.size(F.collect_set("ticket_id").over(w_event_at_date_month)).alias("distinct_ticket_id_in_event_at_date_month"),
        F.size(F.collect_set("agent_team").over(w_event_at_date_month)).alias("distinct_agent_team_in_event_at_date_month"),
        F.round(F.avg("ticket_requester_id").over(w_ticket_csat_by_ticket_csat_p), 2).alias("ticket_requester_id_avg_6_prior"),
        F.ntile(4).over(w_actor_ticket_csat_by_ticket).alias("ticket_requester_id_quartile"),
        F.row_number().over(w_ticket_csat_by_ticket_csat).alias("seq_no"),
        F.first("event_at_date", ignorenulls=True).over(w_ticket_channel_event_at_date).alias("first_known_event_at_date"),
        F.round(F.col("ticket_csat") / F.nullif(F.sum("ticket_csat").over(w_event_at_date_month), F.lit(0)), 4).alias("ticket_csat_share_of_event_at_date_month"),
        F.lag("ticket_requester_id").over(w_ticket_csat_by_ticket_csat).alias("prev_ticket_requester_id"),
        F.size(F.collect_set("agent_tier").over(w_event_at_date_month)).alias("distinct_agent_tier_in_event_at_date_month"),
        F.first("ticket_requester_id", ignorenulls=True).over(w_actor_ticket_csat_by_ticket_2).alias("first_known_ticket_requester_id"),
        F.round(F.cume_dist().over(w_ticket_channel_event_at_date_2), 3).alias("event_at_plus_15m_cume_dist"),
        F.round(F.col("ticket_requester_id") - F.avg("ticket_requester_id").over(w_event_at_date_month), 2).alias("ticket_requester_id_vs_event_at_date_month_avg"),
        F.rank().over(w_ticket_csat_by_ticket_csat).alias("ticket_csat_rank"),
    )
)

windowed = (
    with_trends
    .select(
        "id",
        "ticket_id",
        "event_at_plus_15m",
        "ticket_csat",
        "ticket_requester_id",
        "ticket_channel",
        "agent_tier",
        "distinct_agent_team_in_event_at_date_month",
        "first_known_event_at_date",
        "ticket_csat_share_of_event_at_date_month",
        "prev_ticket_requester_id",
        "first_known_ticket_requester_id",
        "event_at_plus_15m_cume_dist",
        "ticket_requester_id_vs_event_at_date_month_avg",
        "ticket_csat_rank",
        F.round(F.percent_rank().over(w_event_at_date_month_by_disti), 3).alias("distinct_agent_team_in_event_at_date_month_perce"),
        F.nth_value("event_at_plus_15m_cume_dist", 2).over(w_event_at_date_month_by_disti_2).alias("second_event_at_plus_15m_cume_dist"),
        F.lead("event_at_date").over(w_event_at_date_month_by_disti_3).alias("next_event_at_date"),
        F.sum("event_at_plus_15m_cume_dist").over(w_ticket_id_ticket_requester_i).alias("ticket_id_ticket_requester_id_quartile_sum_event"),
    )
)

result = (
    windowed
    .select(
        "id",
        "ticket_csat",
        "ticket_requester_id",
        "ticket_channel",
        "ticket_csat_share_of_event_at_date_month",
        "prev_ticket_requester_id",
        "first_known_ticket_requester_id",
        "event_at_plus_15m_cume_dist",
        "ticket_requester_id_vs_event_at_date_month_avg",
        "second_event_at_plus_15m_cume_dist",
        "next_event_at_date",
        F.date_trunc("month", F.col("event_at_plus_15m")).alias("event_at_plus_15m_month"),
    )
    .orderBy(F.col("ticket_csat").desc_nulls_first())
)
