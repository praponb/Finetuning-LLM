from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id").orderBy("read_at", "id")
w_site_id_running = Window.partitionBy("site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned_readings = (
    df_meter_readings
    .filter(F.col("voltage").between(218.4, 236.7))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.concat(F.col("quality"), F.lit("#"), F.col("id").cast("string")).alias("quality_ref"),
    )
)

with_prev = cleaned_readings.withColumn("prev_read_at", F.lag("read_at").over(w_site_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_read_at").isNull() | ((F.unix_timestamp("read_at") - F.unix_timestamp("prev_read_at")) / 60 > 30), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("starts_session").over(w_site_id_running))

sessions = (
    sessionized
    .groupBy("site_id", "session_no")
    .agg(
        F.min("read_at").alias("session_start"),
        F.max("read_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sum("kwh").alias("session_kwh"),
        F.round((F.unix_timestamp(F.max("read_at")) - F.unix_timestamp(F.min("read_at"))) / 60, 1).alias("duration_min"),
    )
)

annotated = (
    sessions
    .select(
        "*",
        F.when(F.col("duration_min") >= 192.0, "A").when(F.col("duration_min") >= 48.0, "B").otherwise("C").alias("duration_min_tier"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("session_start")).alias("days_since_session_start"),
        F.to_date(F.col("session_start")).alias("session_start_date"),
    )
)

result = (
    annotated
    .select(
        "site_id",
        "session_start",
        "session_end",
        "event_count",
        "session_kwh",
        "duration_min",
        "duration_min_tier",
        "session_start_date",
    )
    .orderBy(F.col("event_count").asc_nulls_last())
)
