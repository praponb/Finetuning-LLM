from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_by_event_count_tie = Window.partitionBy("ticket_id").orderBy("event_count")
w_ticket_id_by_event_count_ful = Window.partitionBy("ticket_id").orderBy("event_count", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_count_2 = Window.partitionBy("event_count_2")

recent_events = (
    df_ticket_events
    .filter(F.col("event_type") != "reopened")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_format(F.col("event_at"), "yyyy-MM").alias("event_at_label"),
        F.quarter(F.col("event_at")).alias("event_at_quarter"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.count("*").alias("event_count"), F.count("*").alias("event_count_2"))
)

summarized_rows = (
    recent_events.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        F.coalesce(F.col("t.event_count_2"), F.lit(0)).alias("event_count_2"),
    )
)

sequenced = (
    summarized_rows
    .select(
        "*",
        F.round(F.percent_rank().over(w_ticket_id_by_event_count_tie), 3).alias("event_count_percent_rank"),
        F.nth_value("event_at", 2).over(w_ticket_id_by_event_count_ful).alias("second_event_at"),
        F.size(F.collect_set("ticket_id").over(w_event_count_2)).alias("distinct_ticket_id_in_event_count_2"),
    )
)

result = (
    sequenced
    .groupBy("event_at_quarter")
    .pivot("event_type", ["escalated", "replied", "reopened", "created"])
    .agg(F.sum("distinct_ticket_id_in_event_count_2"))
)
