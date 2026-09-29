from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality").orderBy("read_at", "id")
w_quality_running = Window.partitionBy("quality").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_count_quality_by_durat = Window.partitionBy("event_count", "quality").orderBy("duration_min", "quality", "session_no")
w_event_count_quality = Window.partitionBy("event_count", "quality")
w_quality_by_session_end = Window.partitionBy("quality").orderBy("session_end", "quality", "session_no")

scoped_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
    )
)

with_prev = scoped_readings.withColumn("prev_read_at", F.lag("read_at").over(w_quality))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_read_at").isNull() | ((F.unix_timestamp("read_at") - F.unix_timestamp("prev_read_at")) / 60 > 60), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_quality_running))

per_session = (
    sessionized
    .groupBy("quality", "session_no")
    .agg(
        F.min("read_at").alias("session_start"),
        F.max("read_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sum("voltage").alias("session_voltage"),
        F.round((F.unix_timestamp(F.max("read_at")) - F.unix_timestamp(F.min("read_at"))) / 60, 1).alias("duration_min"),
    )
)

metrics = (
    per_session
    .select(
        "*",
        F.round(F.percent_rank().over(w_event_count_quality_by_durat), 3).alias("duration_min_percent_rank"),
        F.max("duration_min").over(w_event_count_quality).alias("event_count_quality_max_duration_min"),
        F.rank().over(w_quality_by_session_end).alias("session_end_rank"),
        F.round(F.col("session_voltage") / F.nullif(F.sum("session_voltage").over(w_event_count_quality), F.lit(0)), 4).alias("session_voltage_share_of_event_count_quality"),
    )
)

result = (
    metrics
    .select(
        "quality",
        "session_no",
        "session_start",
        "event_count",
        "session_voltage",
        "duration_min",
        "duration_min_percent_rank",
        "event_count_quality_max_duration_min",
        "session_end_rank",
        "session_voltage_share_of_event_count_quality",
    )
)
