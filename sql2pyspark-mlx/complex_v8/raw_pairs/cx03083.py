from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at_dayofweek_ru = Window.orderBy("event_at_dayofweek", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_actor_ticket_closed_at_year = Window.partitionBy("actor", F.year("ticket_closed_at"))
w_ticket_id_by_event_at_full = Window.partitionBy("ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_days_since_event_at_t = Window.orderBy(F.col("days_since_event_at").desc())
w_all_by_days_since_event_at_m = Window.orderBy(F.col("days_since_event_at").desc(), "id").rowsBetween(-6, Window.currentRow)
w_all_by_days_since_event_at = Window.orderBy(F.col("days_since_event_at").desc(), "id")
w_distinct_event_type_in_actor = Window.partitionBy("distinct_event_type_in_actor_ticket_closed_at_ye")
w_last_event_at_month_prev_eve = Window.partitionBy(F.month("last_event_at"), F.month("prev_event_at")).orderBy("days_since_event_at_percent_rank", "id").rowsBetween(-3, -1)
w_all_by_event_at = Window.orderBy(F.col("event_at").desc(), "id")

filtered_events = (
    df_ticket_events
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.dayofweek(F.col("event_at")).alias("event_at_dayofweek"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("event_at")).alias("days_since_event_at"),
    )
)

enriched_ticket = (
    filtered_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.*",
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("su.team").alias("agent_team"),
    )
)

ranked = (
    enriched_ticket
    .select(
        "*",
        F.max("event_at_dayofweek").over(w_all_by_event_at_dayofweek_ru).alias("max_event_at_dayofweek_so_far"),
        F.size(F.collect_set("agent_team").over(w_actor_ticket_closed_at_year)).alias("distinct_agent_team_in_actor_ticket_closed_at_ye"),
        F.round(F.avg("days_since_event_at").over(w_actor_ticket_closed_at_year), 2).alias("actor_ticket_closed_at_year_avg_days_since_event"),
        F.last("event_at").over(w_ticket_id_by_event_at_full).alias("last_event_at"),
        F.round(F.percent_rank().over(w_all_by_days_since_event_at_t), 3).alias("days_since_event_at_percent_rank"),
        F.max("days_since_event_at").over(w_all_by_days_since_event_at_m).alias("days_since_event_at_max_7"),
        F.size(F.collect_set("event_type").over(w_actor_ticket_closed_at_year)).alias("distinct_event_type_in_actor_ticket_closed_at_ye"),
        F.last("event_at", ignorenulls=True).over(w_all_by_event_at_dayofweek_ru).alias("last_known_event_at"),
        F.count(F.lit(1)).over(w_actor_ticket_closed_at_year).alias("rows_in_actor_ticket_closed_at_year"),
        F.round(F.col("event_at_dayofweek") - F.avg("event_at_dayofweek").over(w_actor_ticket_closed_at_year), 2).alias("event_at_dayofweek_vs_actor_ticket_closed_at_yea"),
        F.round(F.avg("ticket_requester_id").over(w_actor_ticket_closed_at_year), 2).alias("actor_ticket_closed_at_year_avg_ticket_requester"),
        F.lag("event_at").over(w_all_by_days_since_event_at).alias("prev_event_at"),
    )
)

derived = (
    ranked
    .filter(F.col("actor").isin("customer", "agent"))
    .select(
        "*",
        F.substring(F.trim(F.col("agent_team")), 1, 4).alias("agent_team_prefix"),
        F.length(F.trim(F.col("agent_team"))).alias("agent_team_len"),
    )
)

metrics = (
    derived
    .select(
        "*",
        F.round(F.avg("days_since_event_at").over(w_distinct_event_type_in_actor), 2).alias("distinct_event_type_in_actor_ticket_closed_at_ye_2"),
        F.round(F.avg("days_since_event_at").over(w_last_event_at_month_prev_eve), 2).alias("days_since_event_at_avg_3_prior"),
        F.min("days_since_event_at_max_7").over(w_distinct_event_type_in_actor).alias("distinct_event_type_in_actor_ticket_closed_at_ye_3"),
        F.size(F.collect_set("ticket_id").over(w_distinct_event_type_in_actor)).alias("distinct_ticket_id_in_distinct_event_type_in_act"),
        F.lead("event_at").over(w_all_by_event_at).alias("next_event_at"),
    )
)

result = (
    metrics
    .select(
        "id",
        "event_at",
        "event_type",
        "actor",
        "event_at_dayofweek",
        "days_since_event_at",
        "ticket_closed_at",
        "ticket_requester_id",
        "agent_team",
        "max_event_at_dayofweek_so_far",
        "distinct_agent_team_in_actor_ticket_closed_at_ye",
        "actor_ticket_closed_at_year_avg_days_since_event",
        "days_since_event_at_percent_rank",
        "last_known_event_at",
        "rows_in_actor_ticket_closed_at_year",
        "event_at_dayofweek_vs_actor_ticket_closed_at_yea",
        "actor_ticket_closed_at_year_avg_ticket_requester",
        "prev_event_at",
        "agent_team_prefix",
        "distinct_event_type_in_actor_ticket_closed_at_ye_2",
        "distinct_event_type_in_actor_ticket_closed_at_ye_3",
        "distinct_ticket_id_in_distinct_event_type_in_act",
        "next_event_at",
        F.coalesce(F.col("ticket_requester_id"), F.lit(0)).alias("ticket_requester_id_filled"),
        (F.col("agent_team_len") - F.col("days_since_event_at")).alias("agent_team_len_minus_days_since_event_at"),
    )
    .orderBy(F.col("prev_event_at").desc_nulls_first(), F.col("event_at").desc())
)
