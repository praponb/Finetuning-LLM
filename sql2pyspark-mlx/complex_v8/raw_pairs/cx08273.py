from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality").orderBy("read_at", "id")
w_quality_running = Window.partitionBy("quality").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_readings = (
    df_meter_readings
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
    .withColumn("starts_session", F.when(F.col("prev_read_at").isNull() | ((F.unix_timestamp("read_at") - F.unix_timestamp("prev_read_at")) / 60 > 45), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_quality_running))

sessions = (
    sessionized
    .groupBy("quality", "burst_id")
    .agg(
        F.min("read_at").alias("session_start"),
        F.max("read_at").alias("session_end"),
        F.count("*").alias("events"),
        F.sum("voltage").alias("session_voltage"),
        F.round((F.unix_timestamp(F.max("read_at")) - F.unix_timestamp(F.min("read_at"))) / 60, 1).alias("duration_min"),
    )
)

scored = (
    sessions
    .filter(F.col("burst_id") < 5)
    .select(
        "quality",
        "burst_id",
        "session_start",
        "session_end",
        "events",
        "session_voltage",
        "duration_min",
        F.greatest(F.col("duration_min"), F.col("session_voltage")).alias("greatest_duration_min_session_voltage"),
    )
)

result = (
    scored
    .select(
        "quality",
        "session_start",
        "session_end",
        "events",
        "duration_min",
        F.dayofweek(F.col("session_end")).alias("session_end_dayofweek"),
    )
    .orderBy(F.col("duration_min").desc())
)
