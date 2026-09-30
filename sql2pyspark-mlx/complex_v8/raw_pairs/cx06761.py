from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority_event_at_year = Window.partitionBy("priority", F.year("event_at"))
w_agent_id_by_requester_id = Window.partitionBy("agent_id").orderBy("requester_id", "id", "event_id")
w_event_at_to_date_by_requeste = Window.partitionBy(F.to_date("event_at")).orderBy("requester_id", "id", "event_id")

recent_tickets = (
    df_tickets
    .filter(F.col("requester_id") <= 14)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "tags",
    )
)

detail_rows = (
    recent_tickets.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.priority",
        "s.tags",
        F.col("t.id").alias("event_id"),
        "t.event_at",
        "t.event_type",
    )
)

with_windows = (
    detail_rows
    .select(
        "id",
        "requester_id",
        "agent_id",
        "tags",
        "event_id",
        "event_at",
        F.min("requester_id").over(w_priority_event_at_year).alias("priority_event_at_year_min_requester_id"),
        F.row_number().over(w_agent_id_by_requester_id).alias("seq_no"),
        F.lead("requester_id").over(w_event_at_to_date_by_requeste).alias("next_requester_id"),
    )
)

with_children_events = (
    with_windows.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.tags",
        "s.event_id",
        "s.seq_no",
        "s.next_requester_id",
        F.col("t.id").alias("event_id_2"),
        "t.event_type",
        F.col("t.event_at").alias("event_event_at"),
    )
)

result = (
    with_children_events
    .rollup("event_id_2", "event_id", "event_type")
    .agg(
        F.countDistinct("event_id_2").alias("distinct_event_id_2s"),
        F.count("*").alias("n_rows"),
    )
    .select(
        "event_id_2",
        "event_id",
        F.coalesce(F.col("event_type"), F.lit("ALL")).alias("event_type"),
        "distinct_event_id_2s",
        "n_rows",
    )
    .orderBy(
        F.col("event_id_2").asc_nulls_last(),
        F.col("event_id").asc_nulls_last(),
        F.col("event_type").asc_nulls_last(),
    )
)
