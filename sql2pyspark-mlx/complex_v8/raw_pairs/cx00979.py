from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type = Window.partitionBy("event_type").orderBy("event_at", "id")
w_event_type_running = Window.partitionBy("event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_session_no_by_session_end_ru = Window.partitionBy("session_no").orderBy("session_end", "event_type", "session_no").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_type_event_count = Window.partitionBy("event_type", "event_count")
w_session_no_by_session_start = Window.partitionBy("session_no").orderBy(F.col("session_start").desc(), "event_type", "session_no")
w_session_no_event_type = Window.partitionBy("session_no", "event_type")
w_session_no_by_session_start_2 = Window.partitionBy("session_no").orderBy(F.col("session_start").desc(), "event_type", "session_no").rowsBetween(-3, -1)

cleaned_events = (
    df_ticket_events
    .filter(~F.col("actor").isin("system", "customer"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("event_at")).alias("days_since_event_at"),
    )
)

with_prev = cleaned_events.withColumn("prev_event_at", F.lag("event_at").over(w_event_type))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_event_at").isNull() | ((F.unix_timestamp("event_at") - F.unix_timestamp("prev_event_at")) / 60 > 120), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("starts_session").over(w_event_type_running))

session_summary = (
    sessionized
    .groupBy("event_type", "session_no")
    .agg(
        F.min("event_at").alias("session_start"),
        F.max("event_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.round((F.unix_timestamp(F.max("event_at")) - F.unix_timestamp(F.min("event_at"))) / 60, 1).alias("duration_min"),
    )
)

metrics = (
    session_summary
    .select(
        "event_type",
        "session_no",
        "session_start",
        "session_end",
        F.last("session_no", ignorenulls=True).over(w_session_no_by_session_end_ru).alias("last_known_session_no"),
        F.round(F.avg("event_count").over(w_event_type_event_count), 2).alias("event_type_event_count_avg_event_count"),
    )
)

sequenced = (
    metrics
    .select(
        "event_type",
        "session_no",
        "session_start",
        "last_known_session_no",
        "event_type_event_count_avg_event_count",
        F.round(F.cume_dist().over(w_session_no_by_session_start), 3).alias("session_start_cume_dist"),
        F.lead("last_known_session_no").over(w_session_no_by_session_start).alias("next_last_known_session_no"),
        F.lead("session_start", 2).over(w_session_no_by_session_start).alias("next2_session_start"),
        F.lag("session_no", 1, 0).over(w_session_no_by_session_start).alias("prev_session_no"),
        F.count(F.lit(1)).over(w_session_no_event_type).alias("rows_in_session_no_event_type"),
        F.round(F.avg("session_no").over(w_session_no_by_session_start_2), 2).alias("session_no_avg_3_prior"),
    )
)

result = (
    sequenced
    .filter(F.col("event_type") == "reopened")
    .select(
        "event_type",
        "session_start",
        "last_known_session_no",
        "event_type_event_count_avg_event_count",
        "session_start_cume_dist",
        "next_last_known_session_no",
        "prev_session_no",
        "rows_in_session_no_event_type",
        "session_no_avg_3_prior",
    )
    .orderBy(F.col("session_no_avg_3_prior").desc(), "session_start_cume_dist")
)
