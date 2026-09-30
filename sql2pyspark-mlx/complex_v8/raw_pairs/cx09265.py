from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_closed_at_month_agent_id_by = Window.partitionBy(F.date_trunc("month", "closed_at"), "agent_id").orderBy(F.col("opened_at").desc(), "id")
w_closed_at_month_agent_id_by_2 = Window.partitionBy(F.date_trunc("month", "closed_at"), "agent_id").orderBy(F.col("opened_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_closed_at_month_csat = Window.partitionBy(F.month("closed_at"), "csat")

scoped_tickets = (
    df_tickets
    .filter(F.col("opened_at") >= F.lit("2025-01-25 08:00:00").cast("timestamp"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "csat",
        F.round(F.col("requester_id") / F.nullif(F.col("csat"), F.lit(0)), 3).alias("requester_id_per_csat"),
    )
)

sequenced = (
    scoped_tickets
    .select(
        "*",
        F.row_number().over(w_closed_at_month_agent_id_by).alias("opened_at_seq"),
        F.count(F.lit(1)).over(w_closed_at_month_agent_id_by_2).alias("running_count"),
        F.count(F.lit(1)).over(w_closed_at_month_csat).alias("rows_in_closed_at_month_csat"),
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_closed_at_month_csat), F.lit(0)), 4).alias("requester_id_share_of_closed_at_month_csat"),
    )
)

exploded_detail_events = (
    sequenced.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.csat",
        "s.requester_id_per_csat",
        "s.opened_at_seq",
        "s.running_count",
        "s.rows_in_closed_at_month_csat",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        "t.event_at",
        "t.event_type",
    )
)

result = (
    exploded_detail_events
    .select(
        "id",
        "event_id",
        "rows_in_closed_at_month_csat",
        "csat",
        "opened_at_seq",
        "running_count",
    )
    .unpivot(
        ids=["id", "event_id"],
        values=["rows_in_closed_at_month_csat", "csat", "opened_at_seq", "running_count"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .orderBy("id", "event_id", "metric")
)
