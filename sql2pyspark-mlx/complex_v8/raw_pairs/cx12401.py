from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type = Window.partitionBy("event_type").orderBy("event_at", "id")
w_event_type_running = Window.partitionBy("event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_session_end_month_to_date_by = Window.partitionBy(F.to_date("session_end_month")).orderBy("distinct_event_types", "session_no", "session_end_month", "session_start_month").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_session_end_month_to_date_by_2 = Window.partitionBy(F.to_date("session_end_month")).orderBy("distinct_event_types")
w_session_no = Window.partitionBy("session_no")

prepared = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_prev = prepared.withColumn("prev_event_at", F.lag("event_at").over(w_event_type))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_event_at").isNull() | ((F.unix_timestamp("event_at") - F.unix_timestamp("prev_event_at")) / 60 > 45), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("starts_session").over(w_event_type_running))

per_session = (
    sessionized
    .groupBy("event_type", "session_no")
    .agg(
        F.min("event_at").alias("session_start"),
        F.max("event_at").alias("session_end"),
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("event_at")) - F.unix_timestamp(F.min("event_at"))) / 60, 1).alias("duration_min"),
    )
)

summary = (
    per_session
    .groupBy(
        "session_no",
        F.date_trunc("month", "session_end").alias("session_end_month"),
        F.date_trunc("month", "session_start").alias("session_start_month"),
    )
    .agg(
        F.countDistinct("event_type").alias("distinct_event_types"),
        F.min("events").alias("min_events"),
        F.avg("session_no").alias("avg_session_no_check"),
    )
    .filter(F.col("avg_session_no_check") > 2)
    .drop("avg_session_no_check")
)

windowed = (
    summary
    .select(
        "session_no",
        "session_end_month",
        "session_start_month",
        "distinct_event_types",
        "min_events",
        F.last("session_no", ignorenulls=True).over(w_session_end_month_to_date_by).alias("last_known_session_no"),
        F.round(F.cume_dist().over(w_session_end_month_to_date_by_2), 3).alias("distinct_event_types_cume_dist"),
        F.count(F.lit(1)).over(w_session_no).alias("rows_in_session_no"),
        F.round(F.col("session_no") - F.avg("session_no").over(w_session_no), 2).alias("session_no_vs_session_no_avg"),
    )
)

result = (
    windowed
    .select(
        "session_no",
        "session_end_month",
        "last_known_session_no",
        "rows_in_session_no",
        "min_events",
    )
    .unpivot(
        ids=["session_no", "session_end_month"],
        values=["last_known_session_no", "rows_in_session_no", "min_events"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
