from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_requester_id_ties = Window.orderBy(F.col("requester_id").asc_nulls_last())
w_agent_id = Window.partitionBy("agent_id")
w_all_by_requester_id = Window.orderBy(F.col("requester_id").desc_nulls_first(), "id")

recent = (
    df_tickets
    .filter((F.col("priority").isin("P2", "P4", "P1")) & (F.to_date(F.col("closed_at")) < F.lit("2025-02-26").cast("date")))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "csat",
        "tags",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("opened_at")).alias("days_since_opened_at"),
        F.hour(F.col("closed_at")).alias("closed_at_hour"),
    )
)

windowed = (
    recent
    .select(
        "id",
        "priority",
        "csat",
        "closed_at_hour",
        F.round(F.cume_dist().over(w_all_by_requester_id_ties), 3).alias("requester_id_cume_dist"),
        F.round(F.percent_rank().over(w_all_by_requester_id_ties), 3).alias("requester_id_percent_rank"),
        F.sum("closed_at_hour").over(w_agent_id).alias("agent_id_sum_closed_at_hour"),
        F.row_number().over(w_all_by_requester_id).alias("seq_no"),
    )
)

line_level_events = (
    windowed.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.priority",
        "s.csat",
        "s.agent_id_sum_closed_at_hour",
        "s.seq_no",
        F.col("t.id").alias("event_id"),
        "t.event_type",
        F.col("t.actor").alias("event_actor"),
        "t.event_at",
    )
)

enriched = (
    line_level_events
    .select(
        "id",
        "csat",
        "agent_id_sum_closed_at_hour",
        "seq_no",
        "event_id",
        "event_type",
        "event_actor",
        F.concat_ws("-", F.col("event_actor"), F.col("event_type")).alias("event_actor_event_type_label"),
        F.concat_ws(" / ", F.col("event_type"), F.col("event_actor")).alias("event_type_event_actor_label"),
    )
)

result = (
    enriched
    .select(
        "id",
        "agent_id_sum_closed_at_hour",
        "event_id",
        "event_type",
        "event_actor_event_type_label",
        "event_type_event_actor_label",
    )
    .orderBy(
        F.col("event_type").asc_nulls_last(),
        F.col("agent_id_sum_closed_at_hour").desc(),
        "id",
        "event_id",
    )
    .limit(5)
)
