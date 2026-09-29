from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_actor_by_event_at = Window.partitionBy("event_type", "actor").orderBy(F.col("event_at_date").asc_nulls_last(), "id")
w_event_at_year_by_days_since = Window.partitionBy(F.year("event_at")).orderBy("days_since_event_at", "id")
w_all_by_next2_days_since_even = Window.orderBy(F.col("next2_days_since_event_at_filled").desc(), "id")
w_all_by_next2_days_since_even_2 = Window.orderBy(F.col("next2_days_since_event_at_filled").desc())

recent = (
    df_ticket_events
    .filter((F.col("event_at") >= F.lit("2025-02-06 10:00:00").cast("timestamp")) & (F.col("event_type").isin("reopened", "replied", "escalated")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.trim(F.col("event_type"))).alias("event_type_clean"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.to_date(F.col("event_at")).alias("event_at_date"),
    )
)

enriched = (
    recent
    .filter(F.col("actor").isin("customer", "agent"))
    .select(
        "*",
        F.coalesce(F.col("days_since_event_at"), F.lit(0)).alias("days_since_event_at_filled"),
        F.hour(F.col("event_at")).alias("event_at_hour"),
    )
)

windowed = (
    enriched
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "event_type_clean",
        "days_since_event_at",
        "event_at_date",
        "event_at_hour",
        F.round(F.percent_rank().over(w_event_type_actor_by_event_at), 3).alias("event_at_date_percent_rank"),
        F.lead("days_since_event_at_filled", 2).over(w_event_at_year_by_days_since).alias("next2_days_since_event_at_filled"),
    )
)

with_windows = (
    windowed
    .select(
        "id",
        "ticket_id",
        "event_at",
        "days_since_event_at",
        "event_at_hour",
        "event_at_date_percent_rank",
        F.row_number().over(w_all_by_next2_days_since_even).alias("next2_days_since_event_at_filled_seq"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_all_by_next2_days_since_even))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.rank().over(w_all_by_next2_days_since_even_2).alias("next2_days_since_event_at_filled_rank"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.max("event_at").alias("last_event_at"),
        F.max("event_at").alias("last_event_at_2"),
        F.max("event_at").alias("last_event_at_3"),
    )
)

with_counts = (
    with_windows.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        "t.last_event_at",
        "t.last_event_at_2",
        "t.last_event_at_3",
    )
)

result = (
    with_counts
    .select(
        "id",
        "event_at",
        "days_since_event_at",
        "event_at_hour",
        "event_at_date_percent_rank",
        "next2_days_since_event_at_filled_seq",
        "minutes_since_prev_event_at",
        "next2_days_since_event_at_filled_rank",
        "last_event_at_2",
        "last_event_at_3",
        F.round((F.unix_timestamp(F.col("last_event_at_2")) - F.unix_timestamp(F.col("event_at"))) / 3600, 1).alias("hours_event_at_to_last_event_at_2"),
        (F.col("last_event_at") + F.expr("INTERVAL 30 MINUTES")).alias("last_event_at_plus_30m"),
    )
    .orderBy(
        F.col("last_event_at_2").asc_nulls_last(),
        F.col("minutes_since_prev_event_at").asc_nulls_last(),
    )
)
