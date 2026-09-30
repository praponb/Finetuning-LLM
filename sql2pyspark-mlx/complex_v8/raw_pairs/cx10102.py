from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality").orderBy("read_at", "id")
w_quality_running = Window.partitionBy("quality").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        (F.col("read_at") + F.expr("INTERVAL 15 MINUTES")).alias("read_at_plus_15m"),
    )
)

with_prev = cleaned.withColumn("prev_read_at", F.lag("read_at").over(w_quality))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_read_at").isNull() | (F.col("read_at") > F.col("prev_read_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("starts_session").over(w_quality_running))

session_summary = (
    sessionized
    .groupBy("quality", "session_no")
    .agg(
        F.min("read_at").alias("session_start"),
        F.max("read_at").alias("session_end"),
        F.count("*").alias("events"),
        F.sum("kwh").alias("session_kwh"),
        F.round((F.unix_timestamp(F.max("read_at")) - F.unix_timestamp(F.min("read_at"))) / 60, 1).alias("duration_min"),
    )
)

derived = (
    session_summary
    .withColumn("session_start_quarter", F.date_trunc("quarter", F.col("session_start")))
)

stats = (
    derived
    .groupBy(
        F.year("session_start").alias("session_start_year"),
        F.date_trunc("month", "session_start_quarter").alias("session_start_quarter_month"),
        "quality",
    )
    .agg(
        F.round(F.sum("session_kwh") / F.nullif(F.sum("session_no"), F.lit(0)), 3).alias("session_kwh_to_session_no_ratio"),
        F.round(F.sum("duration_min"), 2).alias("total_duration_min"),
        F.count(F.when(~F.col("quality").isin("OK", "BAD"), 1)).alias("n_quality_not_in_ok_bad"),
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("quality") == "BAD", 1)).alias("n_quality_bad"),
    )
)

result = (
    stats
    .select("session_start_year", "session_kwh_to_session_no_ratio", "cnt")
    .orderBy(F.col("session_start_year").desc())
)
