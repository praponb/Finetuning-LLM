from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality").orderBy("read_at", "id")
w_quality_running = Window.partitionBy("quality").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_session_start_month_by_sessi = Window.partitionBy(F.date_trunc("month", "session_start")).orderBy("session_end", "quality", "session_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_session_id = Window.partitionBy("session_id")
w_all_by_duration_min = Window.orderBy(F.col("duration_min").desc(), "quality", "session_id")

filtered_readings = (
    df_meter_readings
    .filter(F.col("voltage") < 233.1)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

with_prev = filtered_readings.withColumn("prev_read_at", F.lag("read_at").over(w_quality))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_read_at").isNull() | ((F.unix_timestamp("read_at") - F.unix_timestamp("prev_read_at")) / 60 > 60), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("starts_session").over(w_quality_running))

per_session = (
    sessionized
    .groupBy("quality", "session_id")
    .agg(
        F.min("read_at").alias("session_start"),
        F.max("read_at").alias("session_end"),
        F.count("*").alias("events"),
        F.sum("kwh").alias("session_kwh"),
        F.round((F.unix_timestamp(F.max("read_at")) - F.unix_timestamp(F.min("read_at"))) / 60, 1).alias("duration_min"),
    )
)

metrics = (
    per_session
    .select(
        "quality",
        "session_id",
        "session_start",
        "session_end",
        F.nth_value("session_start", 2).over(w_session_start_month_by_sessi).alias("second_session_start"),
        F.count(F.lit(1)).over(w_session_id).alias("rows_in_session_id"),
        F.round(F.cume_dist().over(w_all_by_duration_min), 3).alias("duration_min_cume_dist"),
        (F.col("duration_min") - F.lag("duration_min").over(w_all_by_duration_min)).alias("duration_min_change"),
    )
)

result = (
    metrics
    .select(
        "quality",
        "session_id",
        "session_start",
        "session_end",
        "second_session_start",
        "rows_in_session_id",
        "duration_min_cume_dist",
        "duration_min_change",
        F.abs(F.col("session_id") - 4).alias("session_id_distance"),
        F.round((F.unix_timestamp(F.col("session_start")) - F.unix_timestamp(F.col("second_session_start"))) / 60, 1).alias("minutes_second_session_start_to_session_start"),
    )
    .orderBy("session_id")
)
