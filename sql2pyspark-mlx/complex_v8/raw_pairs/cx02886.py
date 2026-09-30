from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality").orderBy("read_at", "id")
w_quality_running = Window.partitionBy("quality").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_n_events_by_row_count = Window.partitionBy("n_events").orderBy("row_count", "quality", "n_events", "session_no")
w_session_no = Window.partitionBy("session_no")
w_n_events_by_row_count_full = Window.partitionBy("n_events").orderBy("row_count", "quality", "n_events", "session_no").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_n_events_by_row_count_runnin = Window.partitionBy("n_events").orderBy("row_count", "quality", "n_events", "session_no").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared_readings = (
    df_meter_readings
    .filter(F.col("quality") == "OK")
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.greatest(F.col("kwh"), F.col("voltage")).alias("greatest_kwh_voltage"),
    )
)

with_prev = prepared_readings.withColumn("prev_read_at", F.lag("read_at").over(w_quality))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_read_at").isNull() | (F.col("read_at") > F.col("prev_read_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w_quality_running))

sessions = (
    sessionized
    .groupBy("quality", "session_no")
    .agg(
        F.min("read_at").alias("session_start"),
        F.max("read_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.sum("kwh").alias("session_kwh"),
        F.round((F.unix_timestamp(F.max("read_at")) - F.unix_timestamp(F.min("read_at"))) / 60, 1).alias("duration_min"),
    )
)

stats = (
    sessions
    .groupBy("quality", "n_events", "session_no")
    .agg(
        F.percentile_approx("session_kwh", 0.5).alias("median_session_kwh"),
        F.count(F.when(~F.col("quality").isin("BAD", "EST"), 1)).alias("n_quality_not_in_bad_est"),
        F.count(F.when(F.col("quality") == "OK", 1)).alias("n_quality_ok"),
        F.count("*").alias("row_count"),
        F.max("n_events").alias("max_n_events"),
        F.round(F.sum("duration_min") / F.nullif(F.sum("n_events"), F.lit(0)), 3).alias("duration_min_to_n_events_ratio"),
    )
    .filter(F.col("row_count") >= 2)
)

metrics = (
    stats
    .select(
        "*",
        F.row_number().over(w_n_events_by_row_count).alias("rn"),
        F.round(F.col("session_no") / F.nullif(F.sum("session_no").over(w_session_no), F.lit(0)), 4).alias("session_no_share_of_session_no"),
        F.first("n_quality_not_in_bad_est").over(w_n_events_by_row_count_full).alias("first_n_quality_not_in_bad_est"),
        F.sum("max_n_events").over(w_n_events_by_row_count_runnin).alias("running_max_n_events"),
        F.count(F.lit(1)).over(w_session_no).alias("rows_in_session_no"),
    )
)

result = (
    metrics
    .select(
        "quality",
        "n_events",
        "n_quality_ok",
        "row_count",
        "max_n_events",
        "duration_min_to_n_events_ratio",
        "rn",
        "first_n_quality_not_in_bad_est",
        "running_max_n_events",
    )
    .orderBy("rn", F.col("n_quality_ok").desc_nulls_first())
)
