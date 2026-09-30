from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_csat_ticket_agent_id = Window.partitionBy("ticket_csat", "ticket_agent_id").orderBy(F.col("ticket_closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_at_to_date_agent_tier = Window.partitionBy(F.to_date("event_at"), "agent_tier")
w_ticket_csat_by_event_at_date = Window.partitionBy("ticket_csat").orderBy("event_at_date", "id")
w_actor_by_ticket_csat_full = Window.partitionBy("actor").orderBy(F.col("ticket_csat").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_csat = Window.partitionBy("ticket_csat")
w_actor_by_ticket_csat_running = Window.partitionBy("actor").orderBy(F.col("ticket_csat").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_actor_by_event_at_full = Window.partitionBy("actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_by_event_at = Window.partitionBy("actor").orderBy("event_at", "id")
w_ticket_csat_ticket_agent_id_2 = Window.partitionBy("ticket_csat", "ticket_agent_id").orderBy(F.col("ticket_closed_at").desc(), "id")
w_last_event_at_date_month_act = Window.partitionBy(F.month("last_event_at_date"), "actor").orderBy("ticket_csat_share_of_ticket_csat")
w_second_ticket_csat_actor = Window.partitionBy("second_ticket_csat", "actor")
w_all_by_event_at_date = Window.orderBy("event_at_date", "id")

recent_events = (
    df_ticket_events
    .filter((F.col("actor") != "agent") & (~F.col("event_type").isin("created", "replied")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.upper(F.trim(F.col("event_type"))).alias("event_type_clean"),
        F.to_date(F.col("event_at")).alias("event_at_date"),
    )
)

with_parent_ticket = (
    recent_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.actor",
        "s.event_type_clean",
        "s.event_at_date",
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("t.csat").alias("ticket_csat"),
        F.col("su.tier").alias("agent_tier"),
    )
)

with_trends = (
    with_parent_ticket
    .select(
        "id",
        "ticket_id",
        "actor",
        "event_at_date",
        "ticket_closed_at",
        "ticket_agent_id",
        "ticket_csat",
        F.first("ticket_closed_at", ignorenulls=True).over(w_ticket_csat_ticket_agent_id).alias("first_known_ticket_closed_at"),
        F.last("event_at_date", ignorenulls=True).over(w_ticket_csat_ticket_agent_id).alias("last_known_event_at_date"),
        F.round(F.col("ticket_csat") / F.nullif(F.sum("ticket_csat").over(w_event_at_to_date_agent_tier), F.lit(0)), 4).alias("ticket_csat_share_of_event_at_to_date_agent_tier"),
        F.round(F.percent_rank().over(w_ticket_csat_by_event_at_date), 3).alias("event_at_date_percent_rank"),
        F.count(F.lit(1)).over(w_event_at_to_date_agent_tier).alias("rows_in_event_at_to_date_agent_tier"),
        F.nth_value("ticket_csat", 2).over(w_actor_by_ticket_csat_full).alias("second_ticket_csat"),
        F.last("event_at_date").over(w_actor_by_ticket_csat_full).alias("last_event_at_date"),
        F.sum("ticket_csat").over(w_ticket_csat).alias("ticket_csat_sum_ticket_csat"),
        F.first("event_at_date", ignorenulls=True).over(w_actor_by_ticket_csat_running).alias("first_known_event_at_date"),
        F.round((F.unix_timestamp("ticket_closed_at") - F.unix_timestamp(F.lag("ticket_closed_at").over(w_ticket_csat_by_event_at_date))) / 60, 1).alias("minutes_since_prev_ticket_closed_at"),
        F.first("event_at").over(w_actor_by_event_at_full).alias("first_event_at"),
        F.rank().over(w_actor_by_event_at).alias("event_at_rank"),
        F.rank().over(w_ticket_csat_ticket_agent_id_2).alias("ticket_closed_at_rank"),
        F.round(F.col("ticket_csat") / F.nullif(F.sum("ticket_csat").over(w_ticket_csat), F.lit(0)), 4).alias("ticket_csat_share_of_ticket_csat"),
    )
)

labeled = (
    with_trends
    .select(
        "*",
        (F.col("first_event_at") + F.expr("INTERVAL 30 MINUTES")).alias("first_event_at_plus_30m"),
        F.dayofweek(F.col("last_event_at_date")).alias("last_event_at_date_dayofweek"),
        F.substring(F.trim(F.col("actor")), 1, 3).alias("actor_prefix"),
    )
)

with_trends_2 = (
    labeled
    .select(
        "id",
        "actor",
        "ticket_closed_at",
        "ticket_agent_id",
        "ticket_csat",
        "last_known_event_at_date",
        "event_at_date_percent_rank",
        "rows_in_event_at_to_date_agent_tier",
        "second_ticket_csat",
        "ticket_csat_sum_ticket_csat",
        "first_known_event_at_date",
        "minutes_since_prev_ticket_closed_at",
        "first_event_at",
        "ticket_closed_at_rank",
        "ticket_csat_share_of_ticket_csat",
        "first_event_at_plus_30m",
        "last_event_at_date_dayofweek",
        "actor_prefix",
        F.dense_rank().over(w_last_event_at_date_month_act).alias("ticket_csat_share_of_ticket_csat_dense_rank"),
        F.sum("ticket_csat").over(w_second_ticket_csat_actor).alias("second_ticket_csat_actor_sum_ticket_csat"),
        F.count(F.lit(1)).over(w_second_ticket_csat_actor).alias("rows_in_second_ticket_csat_actor"),
        F.lead("ticket_csat", 3).over(w_all_by_event_at_date).alias("next3_ticket_csat"),
    )
)

result = (
    with_trends_2
    .select(
        "id",
        "actor",
        "ticket_agent_id",
        "ticket_csat",
        "last_known_event_at_date",
        "event_at_date_percent_rank",
        "second_ticket_csat",
        "ticket_csat_sum_ticket_csat",
        "first_known_event_at_date",
        "minutes_since_prev_ticket_closed_at",
        "first_event_at",
        "ticket_closed_at_rank",
        "ticket_csat_share_of_ticket_csat",
        "first_event_at_plus_30m",
        "last_event_at_date_dayofweek",
        "actor_prefix",
        "second_ticket_csat_actor_sum_ticket_csat",
        "rows_in_second_ticket_csat_actor",
        "next3_ticket_csat",
        F.round(F.col("second_ticket_csat") / F.nullif(F.col("ticket_closed_at_rank"), F.lit(0)), 3).alias("second_ticket_csat_per_ticket_closed_at_rank"),
        F.concat_ws(": ", F.col("actor"), F.col("actor_prefix")).alias("actor_actor_prefix_label"),
    )
)
