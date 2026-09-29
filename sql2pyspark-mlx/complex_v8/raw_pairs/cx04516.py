from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_at_month_agent_hired_o = Window.partitionBy(F.date_trunc("month", "event_at"), F.month("agent_hired_on"))
w_ticket_id = Window.partitionBy("ticket_id")
w_event_at_year_event_at_dayof = Window.partitionBy(F.year("event_at"), "event_at_dayofweek").orderBy(F.col("event_at_dayofweek").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_team_ticket_opened_at = Window.partitionBy("agent_team", F.month("ticket_opened_at"))
w_all_by_ticket_opened_at = Window.orderBy(F.col("ticket_opened_at").desc(), "id")
w_agent_team_event_type_by_tic = Window.partitionBy("agent_team", "event_type").orderBy(F.col("ticket_opened_at").desc())
w_all_by_event_at_ties = Window.orderBy("event_at")
w_event_at_year_event_at_dayof_2 = Window.partitionBy(F.year("event_at"), "event_at_dayofweek").orderBy(F.col("event_at_dayofweek").asc_nulls_last(), "id").rowsBetween(-3, Window.currentRow)
w_all_by_event_at_prior_2 = Window.orderBy(F.col("event_at").desc_nulls_first(), "id").rowsBetween(-2, -1)
w_agent_team_event_type_by_tic_2 = Window.partitionBy("agent_team", "event_type").orderBy(F.col("ticket_opened_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_opened_at_rank = Window.partitionBy("ticket_opened_at_rank")
w_event_type_ticket_opened_at = Window.partitionBy("event_type", "ticket_opened_at_rank").orderBy(F.col("ticket_requester_id_share_of_agent_team_ticket_o").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type_ticket_opened_at_2 = Window.partitionBy("event_type", "ticket_opened_at_rank").orderBy(F.col("ticket_requester_id_share_of_agent_team_ticket_o").desc(), "id")

base = (
    df_ticket_events
    .filter((F.col("ticket_id") >= 42) & (F.col("actor") == "agent"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.dayofweek(F.col("event_at")).alias("event_at_dayofweek"),
        F.substring(F.trim(F.col("actor")), 1, 4).alias("actor_prefix"),
    )
)

enriched_ticket = (
    base.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .filter(F.col("t.requester_id") <= 23)
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.event_at_dayofweek",
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("su.team").alias("agent_team"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

metrics = (
    enriched_ticket
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "event_at_dayofweek",
        "ticket_opened_at",
        "ticket_tags",
        "agent_team",
        F.count(F.lit(1)).over(w_event_at_month_agent_hired_o).alias("rows_in_event_at_month_agent_hired_on_month"),
        F.min("event_at_dayofweek").over(w_ticket_id).alias("ticket_id_min_event_at_dayofweek"),
        F.first("agent_hired_on").over(w_event_at_year_event_at_dayof).alias("first_agent_hired_on"),
        F.round(F.col("ticket_requester_id") / F.nullif(F.sum("ticket_requester_id").over(w_agent_team_ticket_opened_at), F.lit(0)), 4).alias("ticket_requester_id_share_of_agent_team_ticket_o"),
        F.round(F.col("ticket_requester_id") - F.avg("ticket_requester_id").over(w_event_at_month_agent_hired_o), 2).alias("ticket_requester_id_vs_event_at_month_agent_hire"),
        F.max("event_at_dayofweek").over(w_agent_team_ticket_opened_at).alias("agent_team_ticket_opened_at_month_max_event_at_d"),
        F.row_number().over(w_all_by_ticket_opened_at).alias("seq_no"),
        F.rank().over(w_agent_team_event_type_by_tic).alias("ticket_opened_at_rank"),
        F.round(F.cume_dist().over(w_all_by_event_at_ties), 3).alias("event_at_cume_dist"),
        F.round(F.avg("event_at_dayofweek").over(w_event_at_year_event_at_dayof_2), 2).alias("event_at_dayofweek_avg_4"),
        F.round(F.avg("ticket_requester_id").over(w_all_by_event_at_prior_2), 2).alias("ticket_requester_id_avg_2_prior"),
        F.last("ticket_opened_at", ignorenulls=True).over(w_agent_team_event_type_by_tic_2).alias("last_known_ticket_opened_at"),
    )
)

first_rows = (
    metrics
    .filter(F.col("seq_no") <= 2)
    .drop("seq_no")
)

ranked = (
    first_rows
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "event_at_dayofweek",
        "ticket_opened_at",
        "agent_team",
        "ticket_id_min_event_at_dayofweek",
        "ticket_requester_id_share_of_agent_team_ticket_o",
        "agent_team_ticket_opened_at_month_max_event_at_d",
        "event_at_cume_dist",
        "event_at_dayofweek_avg_4",
        "last_known_ticket_opened_at",
        F.count(F.lit(1)).over(w_ticket_opened_at_rank).alias("rows_in_ticket_opened_at_rank"),
        F.first("ticket_opened_at").over(w_event_type_ticket_opened_at).alias("first_ticket_opened_at"),
        F.lead("event_at_dayofweek", 3, 0).over(w_event_type_ticket_opened_at_2).alias("next3_event_at_dayofweek"),
        F.ntile(10).over(w_event_type_ticket_opened_at_2).alias("ticket_requester_id_share_of_agent_team_ticket_o_2"),
    )
)

per_group = (
    ranked
    .groupBy(
        "agent_team",
        "next3_event_at_dayofweek",
        F.year("first_ticket_opened_at").alias("first_ticket_opened_at_year"),
    )
    .agg(
        F.round(F.avg("agent_team_ticket_opened_at_month_max_event_at_d"), 2).alias("avg_agent_team_ticket_opened_at_month_max_event_"),
        F.array_sort(F.collect_set("agent_team")).alias("agent_team_list"),
        F.count(F.when(F.col("event_type") != "created", 1)).alias("n_event_type_created"),
        F.round(100 * F.count(F.when(F.col("agent_team") == "Billing", 1)) / F.count("*"), 1).alias("pct_agent_team_billing"),
    )
)

result = (
    per_group
    .select(
        "agent_team",
        "next3_event_at_dayofweek",
        "first_ticket_opened_at_year",
        "agent_team_list",
        "pct_agent_team_billing",
    )
    .orderBy(F.col("agent_team").desc_nulls_first())
)
