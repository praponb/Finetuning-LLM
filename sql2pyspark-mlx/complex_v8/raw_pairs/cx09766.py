from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_by_agent_hired_on = Window.partitionBy("event_type").orderBy(F.col("agent_hired_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type_by_agent_hired_on_2 = Window.partitionBy("event_type").orderBy(F.col("agent_hired_on").desc(), "id")
w_all_by_event_at_ties = Window.orderBy("event_at")
w_agent_hired_on_month_by_even = Window.partitionBy(F.month("agent_hired_on")).orderBy("event_at", "id")
w_agent_hired_on_month_ticket = Window.partitionBy(F.date_trunc("month", "agent_hired_on"), "ticket_id").orderBy("event_at")
w_event_type_by_agent_hired_on_3 = Window.partitionBy("event_type").orderBy(F.col("agent_hired_on").desc())
w_agent_hired_on_month_ticket_2 = Window.partitionBy(F.date_trunc("month", "agent_hired_on"), "ticket_id").orderBy("event_at", "id")
w_agent_hired_on_month_by_even_2 = Window.partitionBy(F.month("agent_hired_on")).orderBy("event_at")
w_event_at_dense_rank = Window.partitionBy("event_at_dense_rank")
w_next_ticket_closed_at_month = Window.partitionBy(F.month("next_ticket_closed_at"), "event_at_dense_rank").orderBy(F.col("agent_hired_on_rank").desc(), "id")
w_event_type_by_event_at_dense = Window.partitionBy("event_type").orderBy("event_at_dense_rank", "id")
w_agent_hired_on_dense_rank = Window.partitionBy("agent_hired_on_dense_rank")

recent = (
    df_ticket_events
    .filter(F.col("actor").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_format(F.col("event_at"), "yyyy-MM").alias("event_at_label"),
    )
)

with_details_ticket = (
    recent.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.*",
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.closed_at").alias("ticket_closed_at"),
        "su.agent_name",
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

with_windows = (
    with_details_ticket
    .select(
        "*",
        F.last("agent_hired_on").over(w_event_type_by_agent_hired_on).alias("last_agent_hired_on"),
        F.row_number().over(w_event_type_by_agent_hired_on_2).alias("seq_no"),
        F.rank().over(w_all_by_event_at_ties).alias("event_at_rank"),
        F.round(F.percent_rank().over(w_agent_hired_on_month_by_even), 3).alias("event_at_percent_rank"),
        F.dense_rank().over(w_agent_hired_on_month_ticket).alias("event_at_dense_rank"),
        F.dense_rank().over(w_event_type_by_agent_hired_on_3).alias("agent_hired_on_dense_rank"),
        F.datediff(F.col("agent_hired_on"), F.lag("agent_hired_on").over(w_agent_hired_on_month_ticket_2)).alias("days_since_prev_agent_hired_on"),
        F.rank().over(w_event_type_by_agent_hired_on_3).alias("agent_hired_on_rank"),
        F.lead("ticket_closed_at").over(w_event_type_by_agent_hired_on_2).alias("next_ticket_closed_at"),
        F.round(F.percent_rank().over(w_agent_hired_on_month_by_even_2), 3).alias("event_at_percent_rank_2"),
    )
)

leaders = with_windows.filter(F.col("event_at_rank") <= 2)

ranked = (
    leaders
    .select(
        "*",
        F.round(F.col("event_at_percent_rank_2") - F.avg("event_at_percent_rank_2").over(w_event_at_dense_rank), 2).alias("event_at_percent_rank_2_vs_event_at_dense_rank_a"),
        F.dense_rank().over(w_next_ticket_closed_at_month).alias("agent_hired_on_rank_dense_rank"),
        F.round(F.cume_dist().over(w_event_type_by_event_at_dense), 3).alias("event_at_dense_rank_cume_dist"),
        F.round(F.col("event_at_dense_rank") / F.nullif(F.sum("event_at_dense_rank").over(w_event_at_dense_rank), F.lit(0)), 4).alias("event_at_dense_rank_share_of_event_at_dense_rank"),
        F.round(F.col("event_at_rank") - F.avg("event_at_rank").over(w_agent_hired_on_dense_rank), 2).alias("event_at_rank_vs_agent_hired_on_dense_rank_avg"),
    )
)

result = (
    ranked
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "event_at_label",
        "ticket_priority",
        "ticket_closed_at",
        "agent_name",
        "agent_hired_on",
        "seq_no",
        "event_at_rank",
        "event_at_dense_rank",
        "agent_hired_on_dense_rank",
        "days_since_prev_agent_hired_on",
        "agent_hired_on_rank",
        "next_ticket_closed_at",
        "event_at_percent_rank_2",
        "event_at_percent_rank_2_vs_event_at_dense_rank_a",
        "agent_hired_on_rank_dense_rank",
        "event_at_dense_rank_cume_dist",
        F.concat(F.col("actor"), F.lit("#"), F.col("event_at_rank").cast("string")).alias("actor_ref"),
        F.datediff(F.col("agent_hired_on"), F.col("last_agent_hired_on")).alias("days_last_agent_hired_on_to_agent_hired_on"),
    )
)
