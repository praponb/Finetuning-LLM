from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id = Window.partitionBy("ticket_id").orderBy("event_at", "id")
w_ticket_id_running = Window.partitionBy("ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_session_no_session_start_mon = Window.partitionBy("session_no", F.date_trunc("month", "session_start"))
w_all_by_session_start = Window.orderBy(F.col("session_start").desc(), "ticket_id", "session_no")
w_all_by_events = Window.orderBy("events", "ticket_id", "session_no")
w_all_by_events_ties = Window.orderBy("events")

cleaned_events = (
    df_ticket_events
    .filter(F.col("event_at") < F.lit("2025-03-02 09:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.upper(F.trim(F.col("event_type"))).alias("event_type_clean"),
    )
)

with_prev = cleaned_events.withColumn("prev_event_at", F.lag("event_at").over(w_ticket_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_event_at").isNull() | ((F.unix_timestamp("event_at") - F.unix_timestamp("prev_event_at")) / 60 > 30), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("starts_session").over(w_ticket_id_running))

sessions = (
    sessionized
    .groupBy("ticket_id", "session_no")
    .agg(
        F.min("event_at").alias("session_start"),
        F.max("event_at").alias("session_end"),
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("event_at")) - F.unix_timestamp(F.min("event_at"))) / 60, 1).alias("duration_min"),
    )
)

sequenced = (
    sessions
    .select(
        "*",
        F.sum("session_no").over(w_session_no_session_start_mon).alias("session_no_session_start_month_sum_session_no"),
        F.ntile(10).over(w_all_by_session_start).alias("session_start_decile"),
        F.round(F.cume_dist().over(w_all_by_events), 3).alias("events_cume_dist"),
        F.round(F.percent_rank().over(w_all_by_events_ties), 3).alias("events_percent_rank"),
    )
)

result = (
    sequenced
    .groupingSets(
        [["session_no", "ticket_id", "session_start_decile"], ["session_no"], []],
        "session_no", "ticket_id", "session_start_decile",
    )
    .agg(
        F.grouping("session_no").alias("is_total_session_no"),
        F.max("session_start").alias("last_session_start"),
        F.round(F.stddev("events"), 2).alias("stddev_events"),
    )
    .orderBy(
        F.col("session_no").asc_nulls_last(),
        F.col("ticket_id").asc_nulls_last(),
        F.col("session_start_decile").asc_nulls_last(),
    )
)
