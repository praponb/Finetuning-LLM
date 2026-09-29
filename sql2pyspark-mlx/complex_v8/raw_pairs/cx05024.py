from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality").orderBy("read_at", "id")
w_quality_running = Window.partitionBy("quality").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_meter_readings
    .filter(F.col("site_id") >= 5)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

with_prev = cleaned.withColumn("prev_read_at", F.lag("read_at").over(w_quality))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_read_at").isNull() | (F.col("read_at") > F.col("prev_read_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w_quality_running))

session_summary = (
    sessionized
    .groupBy("quality", "session_no")
    .agg(
        F.min("read_at").alias("session_start"),
        F.max("read_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sum("kwh").alias("session_kwh"),
        F.round((F.unix_timestamp(F.max("read_at")) - F.unix_timestamp(F.min("read_at"))) / 60, 1).alias("duration_min"),
    )
)

scored = (
    session_summary
    .filter(~F.col("quality").isin("BAD", "EST"))
    .select(
        "quality",
        "session_no",
        "session_end",
        "session_kwh",
        "duration_min",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("session_start")).alias("days_since_session_start"),
    )
)

result = (
    scored
    .select(
        "quality",
        "session_end",
        "session_kwh",
        "days_since_session_start",
        F.concat(F.col("quality"), F.lit("#"), F.col("session_no").cast("string")).alias("quality_ref"),
        (F.col("session_kwh") + F.col("duration_min")).alias("session_kwh_plus_duration_min"),
    )
    .orderBy(F.col("session_kwh").asc(), F.col("session_end").asc_nulls_last())
)
