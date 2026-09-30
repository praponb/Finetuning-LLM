from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id").orderBy("read_at", "id")
w_site_id_running = Window.partitionBy("site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent = (
    df_meter_readings
    .filter(F.col("read_at") < F.lit("2025-02-12 13:00:00").cast("timestamp"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        (F.col("read_at") + F.expr("INTERVAL 1 DAYS")).alias("read_at_plus_1d"),
    )
)

with_prev = recent.withColumn("prev_read_at", F.lag("read_at").over(w_site_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_read_at").isNull() | (F.col("read_at") > F.col("prev_read_at") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w_site_id_running))

per_session = (
    sessionized
    .groupBy("site_id", "session_no")
    .agg(
        F.min("read_at").alias("session_start"),
        F.max("read_at").alias("session_end"),
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("read_at")) - F.unix_timestamp(F.min("read_at"))) / 60, 1).alias("duration_min"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.sum("kwh").alias("total_reading_kwh"),
        F.max("read_at").alias("last_reading_read_at"),
        F.max("read_at").alias("last_reading_read_at_2"),
    )
)

with_counts = (
    per_session.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "m.total_reading_kwh",
        "m.last_reading_read_at",
        "m.last_reading_read_at_2",
    )
)

result = (
    with_counts
    .filter(F.col("session_no") <= 4)
    .select(
        "site_id",
        "session_no",
        "session_start",
        "session_end",
        "events",
        "duration_min",
        "last_reading_read_at_2",
        (F.floor(F.col("total_reading_kwh") / 100) * 100).alias("total_reading_kwh_bucket"),
    )
    .orderBy(F.col("last_reading_read_at_2").desc())
)
