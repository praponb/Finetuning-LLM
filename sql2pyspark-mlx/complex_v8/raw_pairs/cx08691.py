from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality").orderBy("read_at", "id")
w_quality_running = Window.partitionBy("quality").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_readings = (
    df_meter_readings
    .filter(F.col("site_id") >= 8)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
    )
)

with_prev = scoped_readings.withColumn("prev_read_at", F.lag("read_at").over(w_quality))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_read_at").isNull() | (F.col("read_at") > F.col("prev_read_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("new_session_flag").over(w_quality_running))

derived = (
    sessionized
    .select(
        "*",
        F.abs(F.col("kwh") - 37.0).alias("kwh_distance"),
        F.concat(F.col("quality"), F.lit("#"), F.col("site_id").cast("string")).alias("quality_ref"),
        F.dayofweek(F.col("read_at")).alias("read_at_dayofweek"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count("*").alias("reading_count"),
        F.count("*").alias("reading_count_2"),
    )
)

profiled = (
    derived.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        F.coalesce(F.col("m.reading_count_2"), F.lit(0)).alias("reading_count_2"),
    )
)

result = (
    profiled
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality_slug",
        "prev_read_at",
        "new_session_flag",
        "burst_id",
        "reading_count",
        "reading_count_2",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("prev_read_at")).alias("days_since_prev_read_at"),
        F.concat(F.col("quality_ref"), F.lit("#"), F.col("site_id").cast("string")).alias("quality_ref_ref"),
    )
    .orderBy(F.col("reading_count").asc_nulls_last(), "id")
    .limit(20)
)
