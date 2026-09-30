from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id").orderBy("read_at", "id")
w_site_id_running = Window.partitionBy("site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = df_meter_readings

with_prev = prepared.withColumn("prev_read_at", F.lag("read_at").over(w_site_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_read_at").isNull() | (F.col("read_at") > F.col("prev_read_at") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("starts_session").over(w_site_id_running))

per_session = (
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

filtered = (
    per_session.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.kwh").between(43.1, 82.9)) & (F.col("m.read_at") < F.lit("2025-03-02 09:00:00").cast("timestamp")), "left_semi")
)

result = (
    filtered
    .filter(F.col("session_voltage") <= 903.0)
    .select(
        "site_id",
        "session_id",
        "session_end",
        "event_count",
        "session_voltage",
        F.coalesce(F.col("event_count"), F.lit(0)).alias("event_count_filled"),
    )
    .orderBy(
        F.col("session_end").asc_nulls_last(),
        F.col("event_count_filled").desc(),
    )
)
