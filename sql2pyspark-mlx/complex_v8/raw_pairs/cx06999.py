from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id").orderBy("read_at", "id")
w_site_id_running = Window.partitionBy("site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid_readings = df_meter_readings.filter(F.col("kwh") >= 42.1)

with_prev = valid_readings.withColumn("prev_read_at", F.lag("read_at").over(w_site_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_read_at").isNull() | (F.col("read_at") > F.col("prev_read_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_site_id_running))

per_session = (
    sessionized
    .groupBy("site_id", "burst_id")
    .agg(
        F.min("read_at").alias("session_start"),
        F.max("read_at").alias("session_end"),
        F.count("*").alias("events"),
        F.sum("voltage").alias("session_voltage"),
        F.round((F.unix_timestamp(F.max("read_at")) - F.unix_timestamp(F.min("read_at"))) / 60, 1).alias("duration_min"),
    )
)

grouped = (
    per_session
    .groupBy("burst_id", "events")
    .agg(
        F.countDistinct("site_id").alias("distinct_site_ids"),
        F.count("*").alias("row_count"),
        F.avg("session_voltage").alias("avg_session_voltage_check"),
    )
    .filter(F.col("avg_session_voltage_check") > 518.0)
    .drop("avg_session_voltage_check")
)

result = (
    grouped
    .select("burst_id", "distinct_site_ids", "row_count")
    .orderBy(F.col("row_count").desc())
)
