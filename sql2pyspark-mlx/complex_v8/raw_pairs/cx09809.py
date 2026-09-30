from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality").orderBy("read_at", "id")
w_quality_running = Window.partitionBy("quality").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid_readings = (
    df_meter_readings
    .filter(F.month(F.col("read_at")).isin(1, 2))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.date_trunc("hour", F.col("read_at")).alias("read_at_hour"),
    )
)

with_prev = valid_readings.withColumn("prev_read_at", F.lag("read_at").over(w_quality))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_read_at").isNull() | ((F.unix_timestamp("read_at") - F.unix_timestamp("prev_read_at")) / 60 > 120), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_quality_running))

sessions = (
    sessionized
    .groupBy("quality", "session_id")
    .agg(
        F.min("read_at").alias("session_start"),
        F.max("read_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.round((F.unix_timestamp(F.max("read_at")) - F.unix_timestamp(F.min("read_at"))) / 60, 1).alias("duration_min"),
    )
)

with_activity = (
    sessions
    .crossJoin(sessions.agg(F.avg("session_id").alias("overall_avg_session_id")))
    .filter(F.col("session_id") > F.col("overall_avg_session_id"))
    .drop("overall_avg_session_id")
)

result = (
    with_activity
    .select(
        "quality",
        "session_start",
        "session_end",
        "event_count",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
        F.quarter(F.col("session_start")).alias("session_start_quarter"),
    )
    .orderBy(F.col("session_start_quarter").desc(), F.col("quality_slug").desc())
)
