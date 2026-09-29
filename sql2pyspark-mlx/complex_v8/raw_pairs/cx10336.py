from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_or_unknown = Window.partitionBy("quality_or_unknown").orderBy("read_at", "id")
w_quality_or_unknown_running = Window.partitionBy("quality_or_unknown").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_session_end = Window.orderBy("session_end", "quality_or_unknown", "burst_id")
w_all_by_session_end_ties = Window.orderBy("session_end")
w_n_events = Window.partitionBy("n_events")
w_quality_or_unknown_burst_id = Window.partitionBy("quality_or_unknown", "burst_id").orderBy("session_start", "quality_or_unknown", "burst_id")
w_n_events_burst_id_by_duratio = Window.partitionBy("n_events", "burst_id").orderBy(F.col("duration_min").desc(), "quality_or_unknown", "burst_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_meter_readings
    .filter(F.hour(F.col("read_at")).between(7, 13))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
    )
)

with_prev = valid.withColumn("prev_read_at", F.lag("read_at").over(w_quality_or_unknown))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_read_at").isNull() | (F.col("read_at") > F.col("prev_read_at") + F.expr("INTERVAL 60 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_quality_or_unknown_running))

per_session = (
    sessionized
    .groupBy("quality_or_unknown", "burst_id")
    .agg(
        F.min("read_at").alias("session_start"),
        F.max("read_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.round((F.unix_timestamp(F.max("read_at")) - F.unix_timestamp(F.min("read_at"))) / 60, 1).alias("duration_min"),
    )
)

with_activity = (
    per_session
    .crossJoin(per_session.agg(F.avg("duration_min").alias("overall_avg_duration_min")))
    .filter(F.col("duration_min") > F.col("overall_avg_duration_min"))
    .drop("overall_avg_duration_min")
)

windowed = (
    with_activity
    .select(
        "quality_or_unknown",
        "burst_id",
        "session_start",
        "session_end",
        "n_events",
        "duration_min",
        F.dense_rank().over(w_all_by_session_end).alias("session_end_dense_rank"),
        F.round(F.percent_rank().over(w_all_by_session_end_ties), 3).alias("session_end_percent_rank"),
        F.size(F.collect_set("quality_or_unknown").over(w_n_events)).alias("distinct_quality_or_unknown_in_n_events"),
        F.rank().over(w_quality_or_unknown_burst_id).alias("session_start_rank"),
        F.sum("burst_id").over(w_n_events_burst_id_by_duratio).alias("running_burst_id"),
        F.round(F.col("n_events") - F.avg("n_events").over(w_n_events), 2).alias("n_events_vs_n_events_avg"),
    )
)

result = (
    windowed
    .select(
        "quality_or_unknown",
        "burst_id",
        "distinct_quality_or_unknown_in_n_events",
        "running_burst_id",
        "session_end_dense_rank",
        "n_events",
    )
    .unpivot(
        ids=["quality_or_unknown", "burst_id"],
        values=["distinct_quality_or_unknown_in_n_events", "running_burst_id", "session_end_dense_rank", "n_events"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
)
