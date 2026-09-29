from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id").orderBy("read_at", "id")
w_site_id_running = Window.partitionBy("site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.abs(F.col("voltage") - 224.9).alias("voltage_distance"),
    )
)

with_prev = scoped_readings.withColumn("prev_read_at", F.lag("read_at").over(w_site_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_read_at").isNull() | (F.col("read_at") > F.col("prev_read_at") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("new_session_flag").over(w_site_id_running))

session_summary = (
    sessionized
    .groupBy("site_id", "burst_id")
    .agg(
        F.min("read_at").alias("session_start"),
        F.max("read_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.round((F.unix_timestamp(F.max("read_at")) - F.unix_timestamp(F.min("read_at"))) / 60, 1).alias("duration_min"),
    )
)

result = (
    session_summary
    .select(
        "site_id",
        "burst_id",
        "session_start",
        "event_count",
        "duration_min",
        F.round((F.unix_timestamp(F.col("session_start")) - F.unix_timestamp(F.col("session_end"))) / 3600, 1).alias("hours_session_end_to_session_start"),
    )
    .orderBy(F.col("site_id").asc())
)
