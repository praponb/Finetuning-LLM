from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type = Window.partitionBy("event_type").orderBy("event_at", "id")
w_event_type_running = Window.partitionBy("event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_burst_id_events = Window.partitionBy("burst_id", "events")
w_burst_id_by_session_end_prio = Window.partitionBy("burst_id").orderBy(F.col("session_end").desc(), "event_type", "burst_id").rowsBetween(-6, -1)
w_burst_id_by_session_end = Window.partitionBy("burst_id").orderBy(F.col("session_end").desc(), "event_type", "burst_id")

filtered = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_prev = filtered.withColumn("prev_event_at", F.lag("event_at").over(w_event_type))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_event_at").isNull() | ((F.unix_timestamp("event_at") - F.unix_timestamp("prev_event_at")) / 60 > 60), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_event_type_running))

per_session = (
    sessionized
    .groupBy("event_type", "burst_id")
    .agg(
        F.min("event_at").alias("session_start"),
        F.max("event_at").alias("session_end"),
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("event_at")) - F.unix_timestamp(F.min("event_at"))) / 60, 1).alias("duration_min"),
    )
)

sequenced = (
    per_session
    .select(
        "*",
        F.round(F.avg("duration_min").over(w_burst_id_events), 2).alias("burst_id_events_avg_duration_min"),
        F.count(F.lit(1)).over(w_burst_id_events).alias("rows_in_burst_id_events"),
        F.sum("burst_id").over(w_burst_id_by_session_end_prio).alias("burst_id_sum_6_prior"),
        F.dense_rank().over(w_burst_id_by_session_end).alias("session_end_dense_rank"),
        F.rank().over(w_burst_id_by_session_end).alias("session_end_rank"),
    )
)

derived = (
    sequenced
    .select(
        "*",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("session_end")).alias("days_since_session_end"),
    )
)

result = (
    derived
    .filter(F.col("days_since_session_end") > 280)
    .select(
        "event_type",
        "session_start",
        "rows_in_burst_id_events",
        "burst_id_sum_6_prior",
        "session_end_dense_rank",
        "days_since_session_end",
    )
    .orderBy("burst_id_sum_6_prior")
)
