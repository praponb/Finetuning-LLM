from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("channel").orderBy("last_event_at_2", "id")

recent_tickets = df_tickets.filter(~F.col("priority").isin("P1", "P2"))

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.max("event_at").alias("last_event_at"),
        F.count(F.when(~F.col("event_type").isin("escalated", "resolved"), 1)).alias("event_event_type_count"),
        F.max("event_at").alias("last_event_at_2"),
    )
)

with_stats = (
    recent_tickets.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "t.last_event_at",
        F.coalesce(F.col("t.event_event_type_count"), F.lit(0)).alias("event_event_type_count"),
        "t.last_event_at_2",
    )
)

latest_per_key = (
    with_stats
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

tagged = (
    latest_per_key
    .select(
        "*",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("closed_at")).alias("days_since_closed_at"),
        F.date_trunc("month", F.col("closed_at")).alias("closed_at_month"),
    )
)

result = (
    tagged
    .select(
        "id",
        F.col("days_since_closed_at").alias("days_since_closed_at_v"),
        F.col("csat").alias("m_csat"),
        F.col("requester_id").alias("requester_id_v"),
        F.col("event_event_type_count").alias("event_event_type_count_v"),
    )
    .unpivot(
        ids=["id"],
        values=["days_since_closed_at_v", "m_csat", "requester_id_v", "event_event_type_count_v"],
        variableColumnName="metric",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("id", "metric")
)
