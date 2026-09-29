from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id").orderBy("read_at", "id")
w_site_id_running = Window.partitionBy("site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_readings = (
    df_meter_readings
    .filter(F.col("quality") == "BAD")
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

with_prev = base_readings.withColumn("prev_read_at", F.lag("read_at").over(w_site_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_read_at").isNull() | (F.col("read_at") > F.col("prev_read_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_site_id_running))

sessions = (
    sessionized
    .groupBy("site_id", "session_id")
    .agg(
        F.min("read_at").alias("session_start"),
        F.max("read_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sum("voltage").alias("session_voltage"),
        F.round((F.unix_timestamp(F.max("read_at")) - F.unix_timestamp(F.min("read_at"))) / 60, 1).alias("duration_min"),
    )
)

tagged = (
    sessions
    .select(
        "*",
        F.round(F.col("session_id") / F.nullif(F.col("duration_min"), F.lit(0)), 3).alias("session_id_per_duration_min"),
        F.round(F.col("duration_min") * 1.5, 2).alias("duration_min_adj"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("session_start")).alias("days_since_session_start"),
    )
)

result = (
    tagged
    .filter(F.col("event_count").between(2, 5))
    .select(
        "site_id",
        "session_id",
        "session_start",
        "session_end",
        "event_count",
        "session_voltage",
        "duration_min",
        "duration_min_adj",
        "days_since_session_start",
        F.round((F.unix_timestamp(F.col("session_start")) - F.unix_timestamp(F.col("session_end"))) / 3600, 1).alias("hours_session_end_to_session_start"),
    )
    .orderBy("session_start", "session_voltage")
)
