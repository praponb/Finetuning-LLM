from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id").orderBy("read_at", "id")
w_site_id_running = Window.partitionBy("site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.abs(F.col("kwh") - 37.1).alias("kwh_distance"),
    )
)

with_prev = prepared_readings.withColumn("prev_read_at", F.lag("read_at").over(w_site_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_read_at").isNull() | (F.col("read_at") > F.col("prev_read_at") + F.expr("INTERVAL 60 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("starts_session").over(w_site_id_running))

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.max("read_at").alias("last_reading_read_at"),
        F.max("read_at").alias("last_reading_read_at_2"),
    )
)

with_stats = (
    sessionized.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.last_reading_read_at", "m.last_reading_read_at_2")
)

with_activity = (
    with_stats.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left_semi")
)

result = (
    with_activity
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        "kwh_distance",
        "prev_read_at",
        "last_reading_read_at",
    )
    .orderBy(F.col("prev_read_at").desc(), F.col("read_at").desc())
)
