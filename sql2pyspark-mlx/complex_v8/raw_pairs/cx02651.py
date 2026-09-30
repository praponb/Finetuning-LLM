from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality").orderBy("read_at", "id")
w_quality_running = Window.partitionBy("quality").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_session_id = Window.partitionBy("session_id")
w_quality_session_id_by_durati = Window.partitionBy("quality", "session_id").orderBy(F.col("duration_min").desc(), "quality", "session_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_session_id_by_durati_2 = Window.partitionBy("quality", "session_id").orderBy(F.col("duration_min").desc(), "quality", "session_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_quality_session_id_by_durati_3 = Window.partitionBy("quality", "session_id").orderBy(F.col("duration_min").desc())

base_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
    )
)

with_prev = base_readings.withColumn("prev_read_at", F.lag("read_at").over(w_quality))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_read_at").isNull() | (F.col("read_at") > F.col("prev_read_at") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_quality_running))

sessions = (
    sessionized
    .groupBy("quality", "session_id")
    .agg(
        F.min("read_at").alias("session_start"),
        F.max("read_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sum("kwh").alias("session_kwh"),
        F.round((F.unix_timestamp(F.max("read_at")) - F.unix_timestamp(F.min("read_at"))) / 60, 1).alias("duration_min"),
    )
)

ranked = (
    sessions
    .select(
        "quality",
        "session_id",
        "session_start",
        "session_kwh",
        "duration_min",
        F.count(F.lit(1)).over(w_session_id).alias("rows_in_session_id"),
        F.sum("session_kwh").over(w_quality_session_id_by_durati).alias("running_session_kwh"),
        F.first("session_id").over(w_quality_session_id_by_durati_2).alias("first_session_id"),
        F.min("event_count").over(w_session_id).alias("session_id_min_event_count"),
        F.round(F.col("duration_min") - F.avg("duration_min").over(w_session_id), 2).alias("duration_min_vs_session_id_avg"),
        F.rank().over(w_quality_session_id_by_durati_3).alias("duration_min_rank"),
    )
)

qualified = (
    ranked
    .crossJoin(ranked.agg(F.max("duration_min_rank").alias("overall_max_duration_min_rank")))
    .filter(F.col("duration_min_rank") < F.col("overall_max_duration_min_rank"))
    .drop("overall_max_duration_min_rank")
)

result = (
    qualified
    .select(
        "quality",
        "session_kwh",
        "duration_min",
        "rows_in_session_id",
        "running_session_kwh",
        "duration_min_rank",
        F.concat(F.col("quality"), F.lit("#"), F.col("rows_in_session_id").cast("string")).alias("quality_ref"),
        F.round(F.col("session_kwh") / F.nullif(F.col("session_id"), F.lit(0)), 3).alias("session_kwh_per_session_id"),
    )
)
