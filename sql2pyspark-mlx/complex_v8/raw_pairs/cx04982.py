from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_by_event_at_ties = Window.partitionBy("event_type").orderBy("event_at")
w_event_type_by_event_at = Window.partitionBy("event_type").orderBy("event_at", "id")

filtered_events = (
    df_ticket_events
    .filter(F.col("actor") == "system")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.month(F.col("event_at")).alias("event_at_month"),
        (F.col("event_at") + F.expr("INTERVAL 1 DAYS")).alias("event_at_plus_1d"),
    )
)

scored = (
    filtered_events
    .filter(F.col("event_at") < F.lit("2025-03-02 10:00:00").cast("timestamp"))
    .select(
        "*",
        F.concat(F.col("actor"), F.lit("#"), F.col("event_at_month").cast("string")).alias("actor_ref"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.date_trunc("month", F.col("event_at_plus_1d")).alias("event_at_plus_1d_month"),
    )
)

windowed = (
    scored
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "event_at_month",
        "event_at_plus_1d",
        "actor_ref",
        "days_since_event_at",
        F.rank().over(w_event_type_by_event_at_ties).alias("event_at_rank"),
        F.round((F.unix_timestamp("event_at_plus_1d") - F.unix_timestamp(F.lag("event_at_plus_1d").over(w_event_type_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at_plus_1d"),
    )
)

result = (
    windowed
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "event_at_month",
        "event_at_plus_1d",
        "days_since_event_at",
        "event_at_rank",
        "minutes_since_prev_event_at_plus_1d",
        F.when(F.col("event_at_rank") >= 8, "HIGH").when(F.col("event_at_rank") >= 4, "MEDIUM").otherwise("LOW").alias("event_at_rank_tier"),
    )
)
