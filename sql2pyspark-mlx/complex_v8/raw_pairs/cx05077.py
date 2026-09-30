from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_or_unknown = Window.partitionBy("quality_or_unknown").orderBy("read_at", "id")
w_quality_or_unknown_running = Window.partitionBy("quality_or_unknown").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
    )
)

with_prev = valid_readings.withColumn("prev_read_at", F.lag("read_at").over(w_quality_or_unknown))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_read_at").isNull() | (F.col("read_at") > F.col("prev_read_at") + F.expr("INTERVAL 60 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_quality_or_unknown_running))

sessions = (
    sessionized
    .groupBy("quality_or_unknown", "session_id")
    .agg(
        F.min("read_at").alias("session_start"),
        F.max("read_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.sum("voltage").alias("session_voltage"),
        F.round((F.unix_timestamp(F.max("read_at")) - F.unix_timestamp(F.min("read_at"))) / 60, 1).alias("duration_min"),
    )
)

result = (
    sessions
    .groupBy("session_end", "session_id")
    .pivot("quality_or_unknown", ["EST", "UNKNOWN", "OK"])
    .agg(F.avg("duration_min").alias("avg_v"), F.count("duration_min").alias("n"))
    .orderBy("session_end", "session_id")
)
