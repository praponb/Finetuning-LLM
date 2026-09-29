from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_by_agent_hired_on = Window.partitionBy("ticket_id").orderBy("agent_hired_on", "id")
w_agent_hired_on_month = Window.partitionBy(F.date_trunc("month", "agent_hired_on"))
w_agent_hired_on_month_actor_b = Window.partitionBy(F.date_trunc("month", "agent_hired_on"), "actor").orderBy(F.col("days_since_event_at").desc_nulls_first(), "id").rowsBetween(-2, Window.currentRow)
w_ticket_id_actor = Window.partitionBy("ticket_id", "actor")
w_ticket_id_by_days_since_even = Window.partitionBy("ticket_id").orderBy(F.col("days_since_event_at").asc_nulls_last(), "id")
w_event_type_ticket_id_by_days = Window.partitionBy("event_type", "ticket_id").orderBy("days_since_event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_type_ticket_id_by_days_2 = Window.partitionBy("event_type", "ticket_id").orderBy("days_since_event_at", "id")
w_ticket_id_by_agent_hired_on_2 = Window.partitionBy("ticket_id").orderBy("agent_hired_on")
w_agent_hired_on_month_actor_b_2 = Window.partitionBy(F.date_trunc("month", "agent_hired_on"), "actor").orderBy(F.col("days_since_event_at").desc_nulls_first(), "id")

prepared = (
    df_ticket_events
    .filter((F.col("actor").isin("system", "agent")) & (F.hour(F.col("event_at")).between(7, 13)))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("event_at")).alias("days_since_event_at"),
    )
)

with_parent_ticket = (
    prepared.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        "s.days_since_event_at",
        F.col("t.tags").alias("ticket_tags"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

windowed = (
    with_parent_ticket
    .select(
        "*",
        F.datediff(F.col("agent_hired_on"), F.lag("agent_hired_on").over(w_ticket_id_by_agent_hired_on)).alias("days_since_prev_agent_hired_on"),
        F.count(F.lit(1)).over(w_agent_hired_on_month).alias("rows_in_agent_hired_on_month"),
        F.round(F.avg("days_since_event_at").over(w_agent_hired_on_month_actor_b), 2).alias("days_since_event_at_avg_3"),
        F.count(F.lit(1)).over(w_ticket_id_actor).alias("rows_in_ticket_id_actor"),
        F.lag("agent_hired_on").over(w_ticket_id_by_days_since_even).alias("prev_agent_hired_on"),
        F.last("days_since_event_at", ignorenulls=True).over(w_event_type_ticket_id_by_days).alias("last_known_days_since_event_at"),
        F.row_number().over(w_ticket_id_by_days_since_even).alias("days_since_event_at_seq"),
        F.lag("agent_hired_on").over(w_event_type_ticket_id_by_days_2).alias("prev_agent_hired_on_2"),
        F.max("days_since_event_at").over(w_ticket_id_actor).alias("ticket_id_actor_max_days_since_event_at"),
        F.size(F.collect_set("event_type").over(w_ticket_id_actor)).alias("distinct_event_type_in_ticket_id_actor"),
        F.round(F.percent_rank().over(w_ticket_id_by_agent_hired_on_2), 3).alias("agent_hired_on_percent_rank"),
        F.min("days_since_event_at").over(w_agent_hired_on_month).alias("agent_hired_on_month_min_days_since_event_at"),
        F.round(F.avg("days_since_event_at").over(w_agent_hired_on_month), 2).alias("agent_hired_on_month_avg_days_since_event_at"),
        F.lag("agent_hired_on").over(w_agent_hired_on_month_actor_b_2).alias("prev_agent_hired_on_3"),
        F.sum("days_since_event_at").over(w_agent_hired_on_month).alias("agent_hired_on_month_sum_days_since_event_at"),
    )
)

result = (
    windowed
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "days_since_event_at",
        "ticket_tags",
        "agent_hired_on",
        "rows_in_ticket_id_actor",
        "prev_agent_hired_on",
        "last_known_days_since_event_at",
        "prev_agent_hired_on_2",
        "ticket_id_actor_max_days_since_event_at",
        "distinct_event_type_in_ticket_id_actor",
        "agent_hired_on_percent_rank",
        "prev_agent_hired_on_3",
        "agent_hired_on_month_sum_days_since_event_at",
    )
    .orderBy(
        F.col("agent_hired_on_month_sum_days_since_event_at").asc_nulls_last(),
        "id",
    )
    .limit(10)
)
