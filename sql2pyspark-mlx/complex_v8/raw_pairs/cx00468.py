from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality").orderBy("read_at", "id")
w_quality_running = Window.partitionBy("quality").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_2 = Window.partitionBy("quality")
w_quality_by_duration_min = Window.partitionBy("quality").orderBy(F.col("duration_min").asc_nulls_last(), "quality", "session_id")
w_quality_session_end_to_date = Window.partitionBy("quality", F.to_date("session_end")).orderBy(F.col("duration_min").desc(), "quality", "session_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
    )
)

with_prev = filtered_readings.withColumn("prev_read_at", F.lag("read_at").over(w_quality))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_read_at").isNull() | (F.col("read_at") > F.col("prev_read_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_quality_running))

session_summary = (
    sessionized
    .groupBy("quality", "session_id")
    .agg(
        F.min("read_at").alias("session_start"),
        F.max("read_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.round((F.unix_timestamp(F.max("read_at")) - F.unix_timestamp(F.min("read_at"))) / 60, 1).alias("duration_min"),
    )
)

with_trends = (
    session_summary
    .select(
        "quality",
        "session_id",
        "session_start",
        "session_end",
        "n_events",
        "duration_min",
        F.max("session_id").over(w_quality_2).alias("quality_max_session_id"),
        F.ntile(5).over(w_quality_by_duration_min).alias("duration_min_quintile"),
        F.round(F.cume_dist().over(w_quality_by_duration_min), 3).alias("duration_min_cume_dist"),
        F.last("session_id", ignorenulls=True).over(w_quality_session_end_to_date).alias("last_known_session_id"),
        F.max("duration_min").over(w_quality_2).alias("quality_max_duration_min"),
    )
)

result = (
    with_trends
    .select(
        "quality",
        "session_id",
        "duration_min_cume_dist",
        "duration_min",
        "quality_max_duration_min",
    )
    .unpivot(
        ids=["quality", "session_id"],
        values=["duration_min_cume_dist", "duration_min", "quality_max_duration_min"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
