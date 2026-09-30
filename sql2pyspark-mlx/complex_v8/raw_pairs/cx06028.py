from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality").orderBy("read_at", "id")
w_quality_running = Window.partitionBy("quality").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_n_quality_bad_by_pct_quality = Window.partitionBy("n_quality_bad").orderBy("pct_quality_ok", "session_start_ym").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_n_quality_bad_n_rows_by_n_ro = Window.partitionBy("n_quality_bad", "n_rows").orderBy(F.col("n_rows").desc())

valid_readings = (
    df_meter_readings
    .filter(F.col("read_at") >= F.lit("2025-02-06 14:00:00").cast("timestamp"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.year(F.col("read_at")).alias("read_at_year"),
    )
)

with_prev = valid_readings.withColumn("prev_read_at", F.lag("read_at").over(w_quality))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_read_at").isNull() | ((F.unix_timestamp("read_at") - F.unix_timestamp("prev_read_at")) / 60 > 45), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w_quality_running))

per_session = (
    sessionized
    .groupBy("quality", "session_no")
    .agg(
        F.min("read_at").alias("session_start"),
        F.max("read_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.sum("voltage").alias("session_voltage"),
        F.round((F.unix_timestamp(F.max("read_at")) - F.unix_timestamp(F.min("read_at"))) / 60, 1).alias("duration_min"),
    )
)

totals = (
    per_session
    .filter(F.col("n_events") < 5)
    .groupBy(F.date_format("session_start", "yyyy-MM").alias("session_start_ym"))
    .agg(
        F.round(100 * F.count(F.when(F.col("quality") == "OK", 1)) / F.count("*"), 1).alias("pct_quality_ok"),
        F.count("*").alias("n_rows"),
        F.sum("n_events").alias("total_n_events"),
        F.sum(F.when(F.col("quality") != "BAD", 1).otherwise(0)).alias("n_quality_bad"),
    )
)

with_trends = (
    totals
    .select(
        "*",
        F.first("n_quality_bad", ignorenulls=True).over(w_n_quality_bad_by_pct_quality).alias("first_known_n_quality_bad"),
        F.round(F.percent_rank().over(w_n_quality_bad_n_rows_by_n_ro), 3).alias("n_rows_percent_rank"),
    )
)

result = (
    with_trends
    .select(
        "session_start_ym",
        "total_n_events",
        "first_known_n_quality_bad",
        "n_quality_bad",
        "n_rows",
    )
    .unpivot(
        ids=["session_start_ym"],
        values=["total_n_events", "first_known_n_quality_bad", "n_quality_bad", "n_rows"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("session_start_ym", "metric")
)
