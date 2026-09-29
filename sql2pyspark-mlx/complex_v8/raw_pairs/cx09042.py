from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id").orderBy("read_at", "id")
w_site_id_running = Window.partitionBy("site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.substring(F.trim(F.col("quality")), 1, 3).alias("quality_prefix"),
    )
)

with_prev = recent_readings.withColumn("prev_read_at", F.lag("read_at").over(w_site_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_read_at").isNull() | (F.col("read_at") > F.col("prev_read_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_site_id_running))

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

with_children = (
    sessions.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.site_id",
        "s.session_id",
        "s.session_start",
        "s.session_voltage",
        F.col("m.id").alias("reading_id"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.quality").alias("reading_quality"),
    )
)

tagged = (
    with_children
    .select(
        "site_id",
        "session_id",
        "session_start",
        "reading_id",
        "reading_read_at",
        "reading_quality",
        F.coalesce(F.col("session_id"), F.lit(0)).alias("session_id_filled"),
        F.substring(F.trim(F.col("reading_quality")), 1, 3).alias("reading_quality_prefix"),
    )
)

result = (
    tagged
    .select(
        "site_id",
        "session_id",
        "session_start",
        "reading_id",
        "reading_read_at",
        "reading_quality",
        "session_id_filled",
        "reading_quality_prefix",
        F.substring(F.trim(F.col("reading_quality")), 1, 2).alias("reading_quality_prefix_2"),
        F.abs(F.col("session_id_filled") - 4).alias("session_id_filled_distance"),
    )
    .orderBy(
        F.col("session_id_filled_distance").asc_nulls_last(),
        F.col("session_id").desc(),
    )
)
