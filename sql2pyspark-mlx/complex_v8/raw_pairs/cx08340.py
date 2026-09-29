from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_opened_at = Window.orderBy(F.col("opened_at").desc(), "id")
w_opened_at_month_csat = Window.partitionBy(F.month("opened_at"), "csat")
w_priority_by_opened_at = Window.partitionBy("priority").orderBy(F.col("opened_at").asc_nulls_last(), "id")

scoped_tickets = (
    df_tickets
    .filter((F.col("agent_id") >= 9) & (F.col("requester_id") >= 4))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "csat",
        "tags",
    )
)

ranked = (
    scoped_tickets
    .select(
        "*",
        F.lead("csat", 3).over(w_all_by_opened_at).alias("next3_csat"),
        F.round(F.col("csat") - F.avg("csat").over(w_opened_at_month_csat), 2).alias("csat_vs_opened_at_month_csat_avg"),
        F.dense_rank().over(w_priority_by_opened_at).alias("opened_at_dense_rank"),
    )
)

line_level_events = (
    ranked.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.priority",
        "s.next3_csat",
        "s.csat_vs_opened_at_month_csat_avg",
        "s.opened_at_dense_rank",
        F.col("t.id").alias("event_id"),
        "t.event_at",
        F.col("t.actor").alias("event_actor"),
        "t.event_type",
    )
)

derived = (
    line_level_events
    .select(
        "id",
        "agent_id",
        "event_id",
        "event_at",
        "event_actor",
        "event_type",
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug"),
        F.round(F.col("next3_csat") / F.nullif(F.col("opened_at_dense_rank"), F.lit(0)), 3).alias("next3_csat_per_opened_at_dense_rank"),
        F.coalesce(F.col("opened_at_dense_rank"), F.lit(0)).alias("opened_at_dense_rank_filled"),
    )
)

result = (
    derived
    .groupingSets(
        [["event_type", "agent_id", "event_id"], ["event_type"], []],
        "event_type", "agent_id", "event_id",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.sum("next3_csat_per_opened_at_dense_rank").alias("total_next3_csat_per_opened_at_dense_rank"),
        F.min("next3_csat_per_opened_at_dense_rank").alias("min_next3_csat_per_opened_at_dense_rank"),
    )
    .select(
        F.coalesce(F.col("event_type"), F.lit("ALL")).alias("event_type"),
        "agent_id",
        "event_id",
        "cnt",
        "total_next3_csat_per_opened_at_dense_rank",
        "min_next3_csat_per_opened_at_dense_rank",
    )
)
