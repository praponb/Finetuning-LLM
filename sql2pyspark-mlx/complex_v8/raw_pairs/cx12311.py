from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_team_event_type_by_eve = Window.partitionBy("agent_team", "event_type").orderBy(F.col("event_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_agent_hired_on_full = Window.orderBy("agent_hired_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type_event_at_year = Window.partitionBy("event_type", F.year("event_at"))
w_ticket_id_event_at_month_by = Window.partitionBy("ticket_id", F.date_trunc("month", "event_at")).orderBy("agent_hired_on", "id")
w_all_by_agent_hired_on_moving = Window.orderBy("agent_hired_on", "id").rowsBetween(-6, Window.currentRow)
w_all_by_agent_hired_on_prior = Window.orderBy("agent_hired_on", "id").rowsBetween(-6, -1)
w_ticket_id_event_at_month_by_2 = Window.partitionBy("ticket_id", F.date_trunc("month", "event_at")).orderBy("agent_hired_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_team_event_type_by_eve_2 = Window.partitionBy("agent_team", "event_type").orderBy(F.col("event_at").asc_nulls_last())
w_all_by_agent_hired_on = Window.orderBy("agent_hired_on", "id")
w_agent_team_event_type_by_eve_3 = Window.partitionBy("agent_team", "event_type").orderBy(F.col("event_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared_events = (
    df_ticket_events
    .filter((F.col("event_at") >= F.lit("2025-01-25 10:00:00").cast("timestamp")) & (F.col("ticket_id") >= 20))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_parent_ticket = (
    prepared_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "inner")
    .select(
        "s.*",
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("su.team").alias("agent_team"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

ranked = (
    with_parent_ticket
    .select(
        "*",
        F.last("ticket_requester_id", ignorenulls=True).over(w_agent_team_event_type_by_eve).alias("last_known_ticket_requester_id"),
        F.last("agent_hired_on").over(w_all_by_agent_hired_on_full).alias("last_agent_hired_on"),
        F.count(F.lit(1)).over(w_event_type_event_at_year).alias("rows_in_event_type_event_at_year"),
        F.round(F.cume_dist().over(w_ticket_id_event_at_month_by), 3).alias("agent_hired_on_cume_dist"),
        F.round(F.col("ticket_requester_id") / F.nullif(F.sum("ticket_requester_id").over(w_event_type_event_at_year), F.lit(0)), 4).alias("ticket_requester_id_share_of_event_type_event_at"),
        F.max("ticket_requester_id").over(w_event_type_event_at_year).alias("event_type_event_at_year_max_ticket_requester_id"),
        F.round(F.avg("ticket_requester_id").over(w_all_by_agent_hired_on_moving), 2).alias("ticket_requester_id_avg_7"),
        F.max("ticket_requester_id").over(w_all_by_agent_hired_on_prior).alias("ticket_requester_id_max_6_prior"),
        F.nth_value("ticket_requester_id", 2).over(w_ticket_id_event_at_month_by_2).alias("second_ticket_requester_id"),
        F.last("event_at").over(w_ticket_id_event_at_month_by_2).alias("last_event_at"),
        F.round(F.cume_dist().over(w_agent_team_event_type_by_eve_2), 3).alias("event_at_cume_dist"),
        F.round(F.col("ticket_requester_id") - F.avg("ticket_requester_id").over(w_event_type_event_at_year), 2).alias("ticket_requester_id_vs_event_type_event_at_year_"),
        F.round(F.cume_dist().over(w_all_by_agent_hired_on), 3).alias("agent_hired_on_cume_dist_2"),
        F.dense_rank().over(w_agent_team_event_type_by_eve_2).alias("event_at_dense_rank"),
        F.first("event_at").over(w_agent_team_event_type_by_eve_3).alias("first_event_at"),
        F.ntile(10).over(w_ticket_id_event_at_month_by).alias("agent_hired_on_decile"),
    )
)

first_rows = ranked.filter(F.col("event_at_dense_rank") <= 5)

result = (
    first_rows
    .select(
        "id",
        "event_at",
        "actor",
        "agent_team",
        "agent_hired_on",
        "last_known_ticket_requester_id",
        "last_agent_hired_on",
        "rows_in_event_type_event_at_year",
        "agent_hired_on_cume_dist",
        "event_type_event_at_year_max_ticket_requester_id",
        "ticket_requester_id_avg_7",
        "ticket_requester_id_max_6_prior",
        "last_event_at",
        "ticket_requester_id_vs_event_type_event_at_year_",
        "agent_hired_on_cume_dist_2",
        "first_event_at",
        "agent_hired_on_decile",
    )
    .orderBy("id")
)
