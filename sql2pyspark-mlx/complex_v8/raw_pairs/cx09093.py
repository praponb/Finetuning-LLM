from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_at_month_month_n_rows = Window.partitionBy(F.date_trunc("month", "event_at_month"), "n_rows").orderBy(F.col("first_event_at").desc(), "event_at_month", "event_type", "priority").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_first_event_at_runnin = Window.orderBy("first_event_at", "event_at_month", "event_type", "priority").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_n_rows_first_event_at_month = Window.partitionBy("n_rows", F.date_trunc("month", "first_event_at"))

covered = (
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

grouped = (
    covered
    .groupBy(
        F.date_trunc("month", "event_at").alias("event_at_month"),
        "event_type",
        "priority",
    )
    .agg(
        F.min("event_at").alias("first_event_at"),
        F.round(F.avg("hours_since_open"), 2).alias("avg_hours_since_open"),
        F.count("*").alias("n_rows"),
        F.sum("hours_since_open").alias("sum_hours_since_open_check"),
    )
    .filter(F.col("sum_hours_since_open_check") > 14.4)
    .drop("sum_hours_since_open_check")
)

metrics = (
    grouped
    .select(
        "*",
        F.last("first_event_at").over(w_event_at_month_month_n_rows).alias("last_first_event_at"),
        F.last("n_rows", ignorenulls=True).over(w_all_by_first_event_at_runnin).alias("last_known_n_rows"),
        F.round(F.col("n_rows") - F.avg("n_rows").over(w_n_rows_first_event_at_month), 2).alias("n_rows_vs_n_rows_first_event_at_month_avg"),
    )
)

annotated = (
    metrics
    .select(
        "*",
        (F.floor(F.col("n_rows") / 2) * 2).alias("n_rows_bucket"),
        F.when(F.col("event_type").isin("replied", "reopened"), "group_a").when(F.col("event_type") == "escalated", "group_b").otherwise("other").alias("event_type_group"),
    )
)

result = (
    annotated
    .groupBy("last_first_event_at")
    .pivot("priority", ["P3", "P2", "P4", "P1"])
    .agg(F.max("avg_hours_since_open"))
)
