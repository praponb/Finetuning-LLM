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
        "quality",
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
    )
)

with_prev = recent_readings.withColumn("prev_read_at", F.lag("read_at").over(w_site_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_read_at").isNull() | ((F.unix_timestamp("read_at") - F.unix_timestamp("prev_read_at")) / 60 > 90), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_site_id_running))

session_summary = (
    sessionized
    .groupBy("site_id", "burst_id")
    .agg(
        F.min("read_at").alias("session_start"),
        F.max("read_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sum("voltage").alias("session_voltage"),
        F.round((F.unix_timestamp(F.max("read_at")) - F.unix_timestamp(F.min("read_at"))) / 60, 1).alias("duration_min"),
    )
)

totals = (
    session_summary
    .groupBy(F.date_trunc("month", "session_start").alias("session_start_month"))
    .agg(
        F.round(F.sum("session_voltage") / F.nullif(F.sum("burst_id"), F.lit(0)), 3).alias("session_voltage_to_burst_id_ratio"),
        F.round(F.sum("burst_id") / F.nullif(F.sum("duration_min"), F.lit(0)), 3).alias("burst_id_to_duration_min_ratio"),
        F.percentile_approx("event_count", 0.5).alias("median_event_count"),
    )
)

result = (
    totals
    .select(
        "session_start_month",
        "session_voltage_to_burst_id_ratio",
        "burst_id_to_duration_min_ratio",
    )
    .orderBy("session_voltage_to_burst_id_ratio", "session_start_month")
)
