from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at_year_ties = Window.orderBy("event_at_year")
w_all_by_event_at_year = Window.orderBy("event_at_year", "id")
w_ticket_opened_at_month_event = Window.partitionBy(F.date_trunc("month", "ticket_opened_at"), "event_at_year")
w_event_at_year_ticket_agent_i = Window.partitionBy("event_at_year", "ticket_agent_id")
w_all_by_event_at_year_running = Window.orderBy("event_at_year", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_actor_event_at_year_by_ticke = Window.partitionBy("actor", "event_at_year").orderBy("ticket_opened_at", "id")
w_ticket_agent_id_actor = Window.partitionBy("ticket_agent_id", "actor")
w_actor_ticket_agent_id_by_dis = Window.partitionBy("actor", "ticket_agent_id").orderBy(F.col("distinct_actor_in_ticket_opened_at_month_event_a").asc_nulls_last(), "id")
w_next_event_at_year_2_by_tick = Window.partitionBy("next_event_at_year_2").orderBy(F.col("ticket_opened_at").asc_nulls_last(), "id")
w_all_by_ticket_opened_at = Window.orderBy("ticket_opened_at", "id")

scoped_events = (
    df_ticket_events
    .filter((F.col("event_type") == "replied") & (F.col("ticket_id").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.year(F.col("event_at")).alias("event_at_year"),
    )
)

with_details = (
    scoped_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "inner")
    .filter(F.col("t.opened_at") >= F.lit("2025-01-13 09:00:00").cast("timestamp"))
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.actor",
        "s.event_at_year",
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.agent_id").alias("ticket_agent_id"),
        "su.agent_name",
    )
)

sequenced = (
    with_details
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "ticket_opened_at",
        "ticket_agent_id",
        "agent_name",
        F.dense_rank().over(w_all_by_event_at_year_ties).alias("event_at_year_dense_rank"),
        (F.col("event_at_year") - F.lag("event_at_year").over(w_all_by_event_at_year)).alias("event_at_year_change"),
        F.size(F.collect_set("actor").over(w_ticket_opened_at_month_event)).alias("distinct_actor_in_ticket_opened_at_month_event_a"),
        F.rank().over(w_all_by_event_at_year).alias("event_at_year_rank"),
        F.count(F.lit(1)).over(w_event_at_year_ticket_agent_i).alias("rows_in_event_at_year_ticket_agent_id"),
        F.min("event_at_year").over(w_ticket_opened_at_month_event).alias("ticket_opened_at_month_event_at_year_min_event_a"),
        F.size(F.collect_set("actor").over(w_event_at_year_ticket_agent_i)).alias("distinct_actor_in_event_at_year_ticket_agent_id"),
        F.lead("event_at_year").over(w_all_by_event_at_year).alias("next_event_at_year"),
        F.count(F.lit(1)).over(w_all_by_event_at_year_running).alias("running_count"),
        F.lead("event_at_year", 1, 0).over(w_actor_event_at_year_by_ticke).alias("next_event_at_year_2"),
    )
)

sequenced_2 = (
    sequenced
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "ticket_opened_at",
        "agent_name",
        "event_at_year_dense_rank",
        "event_at_year_change",
        "event_at_year_rank",
        "rows_in_event_at_year_ticket_agent_id",
        "ticket_opened_at_month_event_at_year_min_event_a",
        "distinct_actor_in_event_at_year_ticket_agent_id",
        "running_count",
        "next_event_at_year_2",
        F.round(F.col("event_at_year_dense_rank") / F.nullif(F.sum("event_at_year_dense_rank").over(w_ticket_agent_id_actor), F.lit(0)), 4).alias("event_at_year_dense_rank_share_of_ticket_agent_i"),
        F.lag("event_at", 2).over(w_actor_ticket_agent_id_by_dis).alias("prev2_event_at"),
        F.count(F.lit(1)).over(w_ticket_agent_id_actor).alias("rows_in_ticket_agent_id_actor"),
        F.round((F.unix_timestamp("ticket_opened_at") - F.unix_timestamp(F.lag("ticket_opened_at").over(w_next_event_at_year_2_by_tick))) / 60, 1).alias("minutes_since_prev_ticket_opened_at"),
        F.ntile(10).over(w_all_by_ticket_opened_at).alias("ticket_opened_at_decile"),
        F.round(F.cume_dist().over(w_next_event_at_year_2_by_tick), 3).alias("ticket_opened_at_cume_dist"),
    )
)

result = (
    sequenced_2
    .select(
        "id",
        "event_at",
        "actor",
        "ticket_opened_at",
        "agent_name",
        "event_at_year_dense_rank",
        "event_at_year_change",
        "event_at_year_rank",
        "rows_in_event_at_year_ticket_agent_id",
        "ticket_opened_at_month_event_at_year_min_event_a",
        "distinct_actor_in_event_at_year_ticket_agent_id",
        "running_count",
        "next_event_at_year_2",
        "prev2_event_at",
        "minutes_since_prev_ticket_opened_at",
        "ticket_opened_at_cume_dist",
        F.when(F.col("actor").isin("customer", "system"), "group_a").when(F.col("actor") == "agent", "group_b").otherwise("other").alias("actor_group"),
        F.round(F.col("ticket_opened_at_decile") / F.nullif(F.col("event_at_year_dense_rank"), F.lit(0)), 3).alias("ticket_opened_at_decile_per_event_at_year_dense_"),
    )
    .orderBy(
        "actor",
        F.col("ticket_opened_at_decile_per_event_at_year_dense_").desc_nulls_first(),
    )
)
