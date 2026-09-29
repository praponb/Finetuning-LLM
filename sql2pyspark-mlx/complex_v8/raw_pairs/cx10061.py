from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("site_id", "reading_quality").orderBy(F.col("days_since_read_at_per_kwh").desc(), F.col("id").desc(), F.col("reading_id").desc())

cleaned_readings = (
    df_meter_readings
    .filter(F.col("kwh") > 51.5)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("read_at")).alias("days_since_read_at"),
    )
)

tagged = (
    cleaned_readings
    .select(
        "*",
        F.round(F.col("days_since_read_at") / F.nullif(F.col("kwh"), F.lit(0)), 3).alias("days_since_read_at_per_kwh"),
    )
)

site_id_stats = (
    tagged
    .groupBy("site_id")
    .agg(F.avg("kwh").alias("avg_kwh"))
)

compared = (
    tagged.alias("s")
    .join(site_id_stats.alias("g"), F.col("g.site_id") == F.col("s.site_id"), "inner")
    .filter(F.col("s.kwh") > F.col("g.avg_kwh"))
    .select(
        "s.*",
        "g.avg_kwh",
        F.round(F.col("s.kwh") - F.col("g.avg_kwh"), 2).alias("kwh_minus_avg"),
    )
)

line_level = (
    compared.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.days_since_read_at",
        "s.days_since_read_at_per_kwh",
        F.col("m.id").alias("reading_id"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.kwh").alias("reading_kwh"),
    )
)

latest_per_key = (
    line_level
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

annotated = (
    latest_per_key
    .select(
        "id",
        "site_id",
        "voltage",
        "days_since_read_at",
        "reading_id",
        "reading_read_at",
        "reading_voltage",
        "reading_quality",
        F.quarter(F.col("read_at")).alias("read_at_quarter"),
        F.when(F.col("days_since_read_at") >= 525, "A").when(F.col("days_since_read_at") >= 315, "B").otherwise("C").alias("days_since_read_at_tier"),
        F.substring(F.trim(F.col("reading_quality")), 1, 2).alias("reading_quality_prefix"),
    )
)

stats = (
    annotated
    .filter(F.col("read_at_quarter") > 2)
    .groupBy(
        F.date_trunc("month", "reading_read_at").alias("reading_read_at_month"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.stddev("days_since_read_at"), 2).alias("stddev_days_since_read_at"),
        F.round(100 * F.count(F.when(F.col("reading_quality").isin("EST", "BAD"), 1)) / F.count("*"), 1).alias("pct_reading_quality_in_est_bad"),
        F.sum(F.when(~F.col("reading_quality").isin("OK", "BAD"), F.col("read_at_quarter"))).alias("read_at_quarter_reading_quality_not_in_ok_ba"),
    )
)

result = (
    stats
    .select(
        "reading_read_at_month",
        "stddev_days_since_read_at",
        "pct_reading_quality_in_est_bad",
    )
)
