from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality").orderBy("read_at", "id")
w_quality_running = Window.partitionBy("quality").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.abs(F.col("kwh") - 78.6).alias("kwh_distance"),
    )
)

with_prev = valid_readings.withColumn("prev_read_at", F.lag("read_at").over(w_quality))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_read_at").isNull() | (F.col("read_at") > F.col("prev_read_at") + F.expr("INTERVAL 60 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_quality_running))

with_activity = (
    sessionized.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.hour(F.col("m.read_at")).between(7, 12)), "left_anti")
    .filter(F.col("read_at") < F.lit("2025-02-18 12:00:00").cast("timestamp"))
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.max("voltage").alias("max_reading_voltage"))
)

summarized_rows = (
    with_activity.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.max_reading_voltage")
)

result = (
    summarized_rows
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        "new_session_flag",
    )
    .orderBy("kwh")
)
