from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id").orderBy("read_at", "id")
w_site_id_running = Window.partitionBy("site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_meter_readings
    .filter(F.to_date(F.col("read_at")) < F.lit("2025-02-18").cast("date"))
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

with_prev = valid.withColumn("prev_read_at", F.lag("read_at").over(w_site_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_read_at").isNull() | ((F.unix_timestamp("read_at") - F.unix_timestamp("prev_read_at")) / 60 > 60), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w_site_id_running))

with_children_readings = (
    sessionized.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.voltage") > 216.1), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.quality_len",
        F.col("m.id").alias("reading_id"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.voltage").alias("reading_voltage"),
    )
)

reading_quality_stats = (
    with_children_readings
    .groupBy("reading_quality")
    .agg(F.avg("kwh").alias("avg_kwh"))
)

compared = (
    with_children_readings.alias("s")
    .join(reading_quality_stats.alias("g"), F.col("g.reading_quality") == F.col("s.reading_quality"), "inner")
    .filter(F.col("s.kwh") > F.col("g.avg_kwh"))
    .select(
        "s.*",
        "g.avg_kwh",
        F.round(F.col("s.kwh") - F.col("g.avg_kwh"), 2).alias("kwh_minus_avg"),
    )
)

result = (
    compared
    .rollup("reading_quality", "site_id", "quality_len")
    .agg(F.count("*").alias("n_rows"))
    .orderBy(
        F.col("reading_quality").asc_nulls_last(),
        F.col("site_id").asc_nulls_last(),
        F.col("quality_len").asc_nulls_last(),
    )
)
