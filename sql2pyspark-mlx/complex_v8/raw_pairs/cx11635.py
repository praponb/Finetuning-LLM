from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality").orderBy("read_at", "id")
w_quality_running = Window.partitionBy("quality").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_readings = (
    df_meter_readings
    .filter(F.col("kwh").between(12.2, 110.5))
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
    .withColumn("starts_session", F.when(F.col("prev_read_at").isNull() | (F.col("read_at") > F.col("prev_read_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("starts_session").over(w_quality_running))

per_session = (
    sessionized
    .groupBy("quality", "session_no")
    .agg(
        F.min("read_at").alias("session_start"),
        F.max("read_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sum("kwh").alias("session_kwh"),
        F.round((F.unix_timestamp(F.max("read_at")) - F.unix_timestamp(F.min("read_at"))) / 60, 1).alias("duration_min"),
    )
)

result = (
    per_session
    .select(
        "quality",
        "session_start",
        "session_end",
        "event_count",
        "session_kwh",
        "duration_min",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("session_end")).alias("days_since_session_end"),
        F.to_date(F.col("session_start")).alias("session_start_date"),
    )
    .orderBy(F.col("quality").desc())
)
