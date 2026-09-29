from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("event_type").orderBy(F.col("event_count").desc(), F.col("id").desc())

joined_ranges = (
    df_ticket_events.alias("e")
    .join(
        df_tickets.alias("t"),
        (F.col("t.id") == F.col("e.ticket_id"))
        & (F.col("e.event_at") >= F.col("t.opened_at"))
        & (F.col("e.event_at") <= F.coalesce(F.col("t.closed_at"), F.col("t.opened_at") + F.expr("INTERVAL 1 DAYS"))),
        "inner",
    )
    .select(
        "e.id",
        "e.ticket_id",
        "e.event_at",
        "e.event_type",
        "t.priority",
        "t.agent_id",
        F.round((F.unix_timestamp("e.event_at") - F.unix_timestamp("t.opened_at")) / 3600, 2).alias("hours_since_open"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count("*").alias("event_count"),
        F.count("*").alias("event_count_2"),
        F.min("event_at").alias("first_event_at"),
    )
)

profiled = (
    joined_ranges.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        F.coalesce(F.col("t.event_count_2"), F.lit(0)).alias("event_count_2"),
        "t.first_event_at",
    )
)

latest_per_key = (
    profiled
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

stats = (
    latest_per_key
    .groupBy("event_count", "agent_id")
    .agg(
        F.sum("event_count_2").alias("total_event_count_2"),
        F.round(100 * F.count(F.when(F.col("priority") != "P2", 1)) / F.count("*"), 1).alias("pct_priority_p2"),
        F.sum(F.when(F.col("event_type") == "resolved", F.col("event_count")).otherwise(0)).alias("event_count_event_type_resolved"),
        F.avg("hours_since_open").alias("avg_hours_since_open_check"),
    )
    .filter(F.col("avg_hours_since_open_check") > 18.0)
    .drop("avg_hours_since_open_check")
)

result = (
    stats
    .select(
        "event_count",
        "agent_id",
        "event_count_event_type_resolved",
        "total_event_count_2",
    )
    .unpivot(
        ids=["event_count", "agent_id"],
        values=["event_count_event_type_resolved", "total_event_count_2"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .orderBy("event_count", "agent_id", "metric_name")
)
