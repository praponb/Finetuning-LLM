from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority_csat = Window.partitionBy("priority", "csat")
w_agent_id_by_csat = Window.partitionBy("agent_id").orderBy(F.col("csat").desc(), "id")
w_event_id = Window.partitionBy("event_id")
w_csat_rank_by_csat_rank_ties = Window.partitionBy("csat_rank").orderBy(F.col("csat_rank").desc())
w_event_at_month_csat_rank_by = Window.partitionBy(F.date_trunc("month", "event_at"), "csat_rank").orderBy("event_at", "id", "event_id")

prepared = (
    df_tickets
    .filter(F.month(F.col("opened_at")).isin(1, 2))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "priority",
        "csat",
        F.abs(F.col("requester_id") - 18).alias("requester_id_distance"),
    )
)

ranked = (
    prepared
    .select(
        "id",
        "requester_id",
        "agent_id",
        "priority",
        "requester_id_distance",
        F.count(F.lit(1)).over(w_priority_csat).alias("rows_in_priority_csat"),
        F.rank().over(w_agent_id_by_csat).alias("csat_rank"),
    )
)

line_level_events = (
    ranked.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.to_date(F.col("t.event_at")) < F.lit("2025-02-24").cast("date")), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.priority",
        "s.rows_in_priority_csat",
        "s.csat_rank",
        F.col("t.id").alias("event_id"),
        "t.event_at",
        F.col("t.actor").alias("event_actor"),
    )
)

sequenced = (
    line_level_events
    .select(
        "id",
        "agent_id",
        "rows_in_priority_csat",
        "csat_rank",
        "event_id",
        "event_at",
        "event_actor",
        F.max("rows_in_priority_csat").over(w_event_id).alias("event_id_max_rows_in_priority_csat"),
        F.round(F.percent_rank().over(w_csat_rank_by_csat_rank_ties), 3).alias("csat_rank_percent_rank"),
        F.row_number().over(w_event_at_month_csat_rank_by).alias("rn"),
        F.ntile(4).over(w_event_at_month_csat_rank_by).alias("event_at_quartile"),
        F.round(F.col("csat_rank") / F.nullif(F.sum("csat_rank").over(w_event_id), F.lit(0)), 4).alias("csat_rank_share_of_event_id"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "agent_id",
        "csat_rank",
        "event_id",
        "event_actor",
        "event_id_max_rows_in_priority_csat",
        "csat_rank_percent_rank",
        "rn",
        "csat_rank_share_of_event_id",
    )
    .orderBy(F.col("id").asc_nulls_last())
)
