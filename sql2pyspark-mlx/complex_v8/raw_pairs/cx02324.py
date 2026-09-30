from pyspark.sql import functions as F

filtered_readings = (
    df_meter_readings
    .filter((F.col("voltage") < 238.8) & (F.col("kwh") <= 101.2))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
    )
)

site_id_stats = (
    filtered_readings
    .groupBy("site_id")
    .agg(F.percentile_approx("kwh", 0.5).alias("median_kwh"))
)

outliers = (
    filtered_readings.alias("s")
    .join(site_id_stats.alias("g"), F.col("g.site_id") == F.col("s.site_id"), "inner")
    .filter(F.col("s.kwh") >= F.col("g.median_kwh"))
    .select(
        "s.*",
        "g.median_kwh",
        F.round(F.col("s.kwh") - F.col("g.median_kwh"), 2).alias("kwh_minus_median"),
    )
)

annotated = (
    outliers
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "median_kwh",
        "kwh_minus_median",
        F.round(F.col("median_kwh") * 1.1, 2).alias("median_kwh_adj"),
    )
)

site_id_stats_2 = (
    annotated
    .groupBy("site_id")
    .agg(F.max("median_kwh_adj").alias("max_median_kwh_adj"))
)

vs_group = (
    annotated.alias("s")
    .join(site_id_stats_2.alias("g"), F.col("g.site_id") == F.col("s.site_id"), "inner")
    .filter(F.col("s.median_kwh_adj") == F.col("g.max_median_kwh_adj"))
    .select(
        "s.*",
        "g.max_median_kwh_adj",
        F.round(F.col("s.median_kwh_adj") - F.col("g.max_median_kwh_adj"), 2).alias("median_kwh_adj_minus_max"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.round(F.avg("voltage"), 2).alias("avg_reading_voltage"),
        F.max("voltage").alias("max_reading_voltage"),
    )
)

with_counts = (
    vs_group.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.avg_reading_voltage", "m.max_reading_voltage")
)

summary = (
    with_counts
    .filter(F.col("median_kwh_adj") >= 63.0)
    .groupBy(
        "site_id",
        F.when(F.col("kwh") >= 80.8, "HIGH").when(F.col("kwh") >= 37.0, "MEDIUM").otherwise("LOW").alias("kwh_tier"),
        F.to_date("read_at").alias("read_at_day"),
    )
    .agg(
        F.min("avg_reading_voltage").alias("min_avg_reading_voltage"),
        F.max("avg_reading_voltage").alias("max_avg_reading_voltage"),
        F.min("max_median_kwh_adj").alias("min_max_median_kwh_adj"),
    )
)

line_level = (
    summary.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.site_id",
        "s.kwh_tier",
        "s.read_at_day",
        "s.min_avg_reading_voltage",
        "s.max_avg_reading_voltage",
        "s.min_max_median_kwh_adj",
        F.col("m.id").alias("reading_id"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.kwh").alias("reading_kwh"),
    )
)

result = (
    line_level
    .select(
        "site_id",
        "kwh_tier",
        "read_at_day",
        "min_avg_reading_voltage",
        "max_avg_reading_voltage",
        "min_max_median_kwh_adj",
        "reading_id",
        "reading_read_at",
        "reading_voltage",
        "reading_kwh",
    )
    .orderBy(F.col("min_avg_reading_voltage").desc())
)
