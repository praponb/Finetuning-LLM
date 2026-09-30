from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_by_event_at_week = Window.partitionBy("event_type").orderBy("event_at_week", "id")
w_event_type_by_event_at_week_2 = Window.partitionBy("event_type").orderBy("event_at_week")
w_event_type_by_event_at_week_3 = Window.partitionBy("event_type").orderBy("event_at_week", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base_events = (
    df_ticket_events
    .filter((~F.col("event_type").isin("created", "escalated")) | (F.to_date(F.col("event_at")) < F.lit("2025-02-06").cast("date")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
        F.date_trunc("week", F.col("event_at")).alias("event_at_week"),
    )
)

with_windows = (
    base_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor_slug",
        "event_at_week",
        F.round((F.unix_timestamp("event_at_week") - F.unix_timestamp(F.lag("event_at_week").over(w_event_type_by_event_at_week))) / 60, 1).alias("minutes_since_prev_event_at_week"),
        F.round(F.percent_rank().over(w_event_type_by_event_at_week), 3).alias("event_at_week_percent_rank"),
        F.dense_rank().over(w_event_type_by_event_at_week_2).alias("event_at_week_dense_rank"),
        F.nth_value("event_at", 2).over(w_event_type_by_event_at_week_3).alias("second_event_at"),
        F.ntile(10).over(w_event_type_by_event_at_week).alias("event_at_week_decile"),
    )
)

top_per_group = (
    with_windows
    .filter((F.col("event_at_week_dense_rank") == 1) & (F.col("second_event_at") < F.lit("2025-02-12 13:00:00").cast("timestamp")))
)

screened = (
    top_per_group.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left_semi")
)

result = (
    screened
    .groupBy("event_at_week_decile")
    .pivot("event_type", ["assigned", "created", "reopened", "resolved"])
    .agg(F.avg("event_at_week_percent_rank"))
)
