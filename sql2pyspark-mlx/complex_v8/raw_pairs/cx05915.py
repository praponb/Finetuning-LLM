from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_at_month_event_type = Window.partitionBy(F.date_trunc("month", "event_at"), "event_type")
w_event_at_hour_by_event_at = Window.partitionBy("event_at_hour").orderBy(F.col("event_at").desc_nulls_first(), "id")

valid_events = (
    df_ticket_events
    .filter((F.col("event_type") != "reopened") & (F.col("ticket_id") >= 43))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.hour(F.col("event_at")).alias("event_at_hour"),
        F.concat(F.col("event_type"), F.lit("#"), F.col("id").cast("string")).alias("event_type_ref"),
    )
)

ranked = (
    valid_events
    .select(
        "id",
        "event_at",
        "event_type",
        "actor",
        "days_since_event_at",
        "event_at_hour",
        "event_type_ref",
        F.round(F.col("event_at_hour") - F.avg("event_at_hour").over(w_event_at_month_event_type), 2).alias("event_at_hour_vs_event_at_month_event_type_avg"),
        F.row_number().over(w_event_at_hour_by_event_at).alias("event_at_seq"),
        F.lag("event_at_hour").over(w_event_at_hour_by_event_at).alias("prev_event_at_hour"),
    )
)

labeled = ranked.withColumn("event_at_hour_2", F.date_trunc("hour", F.col("event_at")))

result = (
    labeled
    .select(
        "id",
        "event_at",
        "event_type",
        "actor",
        "days_since_event_at",
        "event_at_hour",
        "event_type_ref",
        "event_at_hour_vs_event_at_month_event_type_avg",
        "event_at_seq",
        "prev_event_at_hour",
        "event_at_hour_2",
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
    )
    .orderBy(F.col("id").asc_nulls_last(), F.col("event_type_or_unknown").asc())
)
