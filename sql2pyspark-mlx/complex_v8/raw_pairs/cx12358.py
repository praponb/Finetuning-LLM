from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at_full = Window.orderBy(F.col("event_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at = Window.orderBy(F.col("event_at").asc_nulls_last(), "id")
w_event_at_to_date_by_event_at = Window.partitionBy(F.to_date("event_at")).orderBy(F.col("event_at").desc())
w_event_at_to_date_by_event_at_2 = Window.partitionBy(F.to_date("event_at")).orderBy(F.col("event_at").desc(), "id")

prepared = (
    df_ticket_events
    .filter(F.col("event_at") < F.lit("2025-02-12 08:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_parent_ticket = (
    prepared.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .filter(F.col("t.priority").isNotNull())
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.priority").alias("ticket_priority"),
    )
)

windowed = (
    with_parent_ticket
    .select(
        "*",
        F.nth_value("event_at", 2).over(w_all_by_event_at_full).alias("second_event_at"),
        F.lead("ticket_opened_at", 2).over(w_all_by_event_at).alias("next2_ticket_opened_at"),
        F.round(F.percent_rank().over(w_event_at_to_date_by_event_at), 3).alias("event_at_percent_rank"),
        F.round((F.unix_timestamp("ticket_opened_at") - F.unix_timestamp(F.lag("ticket_opened_at").over(w_all_by_event_at))) / 60, 1).alias("minutes_since_prev_ticket_opened_at"),
        F.round(F.percent_rank().over(w_event_at_to_date_by_event_at_2), 3).alias("event_at_percent_rank_2"),
    )
)

with_parent_ticket_2 = (
    windowed.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.ticket_opened_at",
        "s.second_event_at",
        "s.next2_ticket_opened_at",
        "s.event_at_percent_rank",
        "s.minutes_since_prev_ticket_opened_at",
        "s.event_at_percent_rank_2",
        F.col("t.priority").alias("ticket_priority_2"),
    )
)

grouped = (
    with_parent_ticket_2
    .groupBy(
        "event_type",
        "ticket_priority_2",
        F.to_date("event_at").alias("event_at_day"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(F.col("ticket_priority_2").isin("P1", "P3", "P4"), 1).otherwise(0)).alias("n_ticket_priority_2_in_p1_p3_p"),
        F.count("*").alias("row_count"),
    )
)

result = (
    grouped
    .select("event_type", "event_at_day")
    .distinct()
    .orderBy(F.col("event_type").asc_nulls_last())
)
