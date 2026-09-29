from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_actor_by_event_at = Window.partitionBy("event_type", "actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_type_actor_by_event_at_2 = Window.partitionBy("event_type", "actor").orderBy("event_at", "id")
w_ticket_id_by_event_at_ties = Window.partitionBy("ticket_id").orderBy("event_at")
w_all_by_event_at = Window.orderBy(F.col("event_at").desc(), "id")
w_ticket_id_by_event_at = Window.partitionBy("ticket_id").orderBy("event_at", "id")
w_event_type_actor_by_event_at_3 = Window.partitionBy("event_type", "actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_tier = Window.partitionBy("agent_tier")
w_all_by_prev_event_at_full = Window.orderBy(F.col("prev_event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_first_event_at_month_next2_e = Window.partitionBy(F.month("first_event_at"), F.date_trunc("month", "next2_event_at"))
w_next3_event_at_to_date_by_ev = Window.partitionBy(F.to_date("next3_event_at")).orderBy("event_at_percent_rank_2", "id")
w_agent_tier_by_first_known_ev = Window.partitionBy("agent_tier").orderBy("first_known_event_at", "id").rowsBetween(-6, Window.currentRow)

scoped = (
    df_ticket_events
    .filter((F.col("event_at") >= F.lit("2025-02-06 12:00:00").cast("timestamp")) & (F.col("actor").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

joined = (
    scoped.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (~F.array_contains(F.col("t.tags"), "login")), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("su.tier").alias("agent_tier"),
    )
)

sequenced = (
    joined
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "ticket_agent_id",
        "agent_tier",
        F.first("event_at", ignorenulls=True).over(w_event_type_actor_by_event_at).alias("first_known_event_at"),
        F.lead("event_at", 3).over(w_event_type_actor_by_event_at_2).alias("next3_event_at"),
        F.round(F.percent_rank().over(w_ticket_id_by_event_at_ties), 3).alias("event_at_percent_rank"),
        F.lag("event_at").over(w_event_type_actor_by_event_at_2).alias("prev_event_at"),
        F.lead("event_at", 2).over(w_all_by_event_at).alias("next2_event_at"),
        F.rank().over(w_ticket_id_by_event_at).alias("event_at_rank"),
        F.first("event_at").over(w_event_type_actor_by_event_at_3).alias("first_event_at"),
        F.round(F.percent_rank().over(w_ticket_id_by_event_at), 3).alias("event_at_percent_rank_2"),
    )
)

with_trends = (
    sequenced
    .select(
        "*",
        F.count(F.lit(1)).over(w_agent_tier).alias("rows_in_agent_tier"),
        F.last("next3_event_at").over(w_all_by_prev_event_at_full).alias("last_next3_event_at"),
        F.count(F.lit(1)).over(w_first_event_at_month_next2_e).alias("rows_in_first_event_at_month_next2_event_at_mont"),
        F.lag("first_known_event_at", 2).over(w_next3_event_at_to_date_by_ev).alias("prev2_first_known_event_at"),
        F.max("event_at_percent_rank").over(w_agent_tier_by_first_known_ev).alias("event_at_percent_rank_max_7"),
        F.size(F.collect_set("ticket_agent_id").over(w_agent_tier)).alias("distinct_ticket_agent_id_in_agent_tier"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "actor",
        "ticket_agent_id",
        "agent_tier",
        "first_known_event_at",
        "next3_event_at",
        "event_at_percent_rank",
        "next2_event_at",
        "event_at_percent_rank_2",
        "rows_in_agent_tier",
        "last_next3_event_at",
        "rows_in_first_event_at_month_next2_event_at_mont",
        "event_at_percent_rank_max_7",
    )
    .orderBy(
        F.col("first_known_event_at").desc_nulls_first(),
        F.col("last_next3_event_at").desc(),
    )
)
