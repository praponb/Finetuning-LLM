from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_by_ticket_csat_running = Window.partitionBy("actor").orderBy(F.col("ticket_csat").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_agent_id = Window.partitionBy("ticket_agent_id")
w_all_by_event_at_ties = Window.orderBy("event_at")
w_actor_by_ticket_csat_ties = Window.partitionBy("actor").orderBy(F.col("ticket_csat").desc())
w_ticket_priority_ticket_agent = Window.partitionBy("ticket_priority", "ticket_agent_id").orderBy(F.col("ticket_csat").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_event_at = Window.orderBy("event_at", "id")
w_actor_by_ticket_csat = Window.partitionBy("actor").orderBy(F.col("ticket_csat").desc(), "id")

filtered_events = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_format(F.col("event_at"), "MMM yyyy").alias("event_at_label"),
    )
)

with_parent_ticket = (
    filtered_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .filter(F.col("t.agent_id") <= 2)
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.actor",
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.csat").alias("ticket_csat"),
    )
)

windowed = (
    with_parent_ticket
    .select(
        "*",
        F.min("ticket_csat").over(w_actor_by_ticket_csat_running).alias("min_ticket_csat_so_far"),
        F.min("ticket_csat").over(w_ticket_agent_id).alias("ticket_agent_id_min_ticket_csat"),
        F.round(F.percent_rank().over(w_all_by_event_at_ties), 3).alias("event_at_percent_rank"),
        F.round(F.percent_rank().over(w_actor_by_ticket_csat_ties), 3).alias("ticket_csat_percent_rank"),
        F.last("event_at", ignorenulls=True).over(w_ticket_priority_ticket_agent).alias("last_known_event_at"),
        F.round(F.col("ticket_csat") - F.avg("ticket_csat").over(w_ticket_agent_id), 2).alias("ticket_csat_vs_ticket_agent_id_avg"),
        F.count(F.lit(1)).over(w_ticket_agent_id).alias("rows_in_ticket_agent_id"),
        F.lead("event_at").over(w_all_by_event_at).alias("next_event_at"),
        F.lead("ticket_csat").over(w_actor_by_ticket_csat).alias("next_ticket_csat"),
        F.size(F.collect_set("ticket_id").over(w_ticket_agent_id)).alias("distinct_ticket_id_in_ticket_agent_id"),
        F.size(F.collect_set("ticket_priority").over(w_ticket_agent_id)).alias("distinct_ticket_priority_in_ticket_agent_id"),
        F.row_number().over(w_actor_by_ticket_csat).alias("seq_no"),
        F.max("ticket_csat").over(w_ticket_priority_ticket_agent).alias("max_ticket_csat_so_far"),
    )
)

result = (
    windowed
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "ticket_priority",
        "ticket_csat",
        "min_ticket_csat_so_far",
        "ticket_agent_id_min_ticket_csat",
        "event_at_percent_rank",
        "last_known_event_at",
        "rows_in_ticket_agent_id",
        "next_event_at",
        "next_ticket_csat",
        "distinct_ticket_priority_in_ticket_agent_id",
        "seq_no",
        "max_ticket_csat_so_far",
        F.when(F.col("actor").isin("customer", "agent"), "group_a").when(F.col("actor") == "system", "group_b").otherwise("other").alias("actor_group"),
        F.to_date(F.col("next_event_at")).alias("next_event_at_date"),
    )
    .orderBy(F.col("rows_in_ticket_agent_id").desc(), F.col("seq_no").desc(), "id")
    .limit(20)
)
