from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality").orderBy("read_at", "id")
w_quality_running = Window.partitionBy("quality").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_read_at_to_date = Window.partitionBy("site_id", F.to_date("read_at"))
w_site_id_starts_session_by_re = Window.partitionBy("site_id", "starts_session").orderBy("read_at")

cleaned = df_meter_readings

with_prev = cleaned.withColumn("prev_read_at", F.lag("read_at").over(w_quality))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_read_at").isNull() | (F.col("read_at") > F.col("prev_read_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_quality_running))

with_activity = (
    sessionized.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.read_at") >= F.lit("2025-01-19 09:00:00").cast("timestamp")), "left_semi")
)

windowed = (
    with_activity
    .withColumn("site_id_read_at_to_date_sum_voltage", F.sum("voltage").over(w_site_id_read_at_to_date))
    .withColumn("read_at_percent_rank", F.round(F.percent_rank().over(w_site_id_starts_session_by_re), 3))
)

result = (
    windowed
    .filter(F.col("read_at") >= F.lit("2025-01-19 09:00:00").cast("timestamp"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        "prev_read_at",
        "starts_session",
        "burst_id",
        "site_id_read_at_to_date_sum_voltage",
        "read_at_percent_rank",
        F.abs(F.col("voltage") - 220.7).alias("voltage_distance"),
    )
    .orderBy("prev_read_at")
)
