from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id").orderBy("read_at", "id")
w_site_id_running = Window.partitionBy("site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
    )
)

with_prev = prepared.withColumn("prev_read_at", F.lag("read_at").over(w_site_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_read_at").isNull() | ((F.unix_timestamp("read_at") - F.unix_timestamp("prev_read_at")) / 60 > 60), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_site_id_running))

session_summary = (
    sessionized
    .groupBy("site_id", "session_id")
    .agg(
        F.min("read_at").alias("session_start"),
        F.max("read_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.round((F.unix_timestamp(F.max("read_at")) - F.unix_timestamp(F.min("read_at"))) / 60, 1).alias("duration_min"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.sum("voltage").alias("total_reading_voltage"),
        F.count("*").alias("reading_count"),
    )
)

with_counts = (
    session_summary.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "m.total_reading_voltage",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
    )
)

without_matches = (
    with_counts.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.upper(F.trim(F.col("si.site_name"))).like("M%")), "left_anti")
)

result = (
    without_matches
    .select(
        "site_id",
        "session_id",
        "session_end",
        "event_count",
        "duration_min",
        "reading_count",
        F.round(F.col("session_id") / F.nullif(F.col("reading_count"), F.lit(0)), 3).alias("session_id_per_reading_count"),
    )
    .orderBy("event_count", F.col("session_id").asc_nulls_last(), "site_id")
    .limit(5)
)
