from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_event_type_by_even = Window.partitionBy("ticket_id", "event_type").orderBy(F.col("event_at_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id_by_event_at_full = Window.partitionBy("ticket_id").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id_by_event_at_runnin = Window.partitionBy("ticket_id").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_event_type_count_event = Window.partitionBy("event_event_type_count", F.month("event_at"))

cleaned = (
    df_ticket_events
    .filter(F.col("event_at") < F.lit("2025-03-08 08:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.to_date(F.col("event_at")).alias("event_at_date"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count(F.when(F.col("event_type") == "assigned", 1)).alias("event_event_type_count"),
    )
)

summarized_rows = (
    cleaned.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_event_type_count"), F.lit(0)).alias("event_event_type_count"),
    )
)

with_trends = (
    summarized_rows
    .select(
        "id",
        "event_type",
        "actor",
        "event_at_date",
        "event_event_type_count",
        F.nth_value("event_at_date", 2).over(w_ticket_id_event_type_by_even).alias("second_event_at_date"),
        F.nth_value("event_at_date", 2).over(w_ticket_id_by_event_at_full).alias("second_event_at_date_2"),
        F.sum("event_event_type_count").over(w_ticket_id_by_event_at_runnin).alias("running_event_event_type_count"),
        F.count(F.lit(1)).over(w_event_event_type_count_event).alias("rows_in_event_event_type_count_event_at_month"),
    )
)

without_matches = (
    with_trends
    .crossJoin(with_trends.agg(F.min("running_event_event_type_count").alias("overall_min_running_event_event_type_count")))
    .filter(F.col("running_event_event_type_count") > F.col("overall_min_running_event_event_type_count"))
    .drop("overall_min_running_event_event_type_count")
)

result = (
    without_matches
    .groupBy("second_event_at_date", "event_event_type_count")
    .pivot("actor", ["customer", "agent"])
    .agg(F.sum("running_event_event_type_count"))
    .orderBy("second_event_at_date", "event_event_type_count")
)
