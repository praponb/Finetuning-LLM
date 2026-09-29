from pyspark.sql import functions as F

prepared = (
    df_meter_readings
    .filter((F.month(F.col("read_at")).isin(1, 2)) & (F.col("quality") == "OK"))
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
    )
)

exploded_detail_readings = (
    prepared.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.quality",
        "s.quality_len",
        F.col("m.id").alias("reading_id"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.quality").alias("reading_quality"),
    )
)

with_details_site = (
    exploded_detail_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .join(df_meter_readings.alias("m"), F.col("m.id") == F.col("s.reading_id"), "left")
    .filter(F.col("si.commissioned_on") >= F.lit("2024-09-15").cast("date"))
    .select(
        "s.id",
        "s.site_id",
        "s.voltage",
        "s.quality",
        "s.quality_len",
        "s.reading_id",
        "s.reading_kwh",
        "s.reading_voltage",
        "s.reading_quality",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("m.read_at").alias("reading_read_at"),
    )
)

grouped = (
    with_details_site
    .filter(F.year(F.col("site_commissioned_on")) == 2024)
    .groupBy("reading_quality", "site_id", "quality_len")
    .agg(
        F.max("reading_read_at").alias("last_reading_read_at"),
        F.min("quality_len").alias("min_quality_len"),
        F.sum("quality_len").alias("total_quality_len"),
        F.round(F.stddev("reading_kwh"), 2).alias("stddev_reading_kwh"),
        F.round(100 * F.count(F.when(F.col("quality").isin("EST", "BAD"), 1)) / F.count("*"), 1).alias("pct_quality_in_est_bad"),
        F.count("*").alias("n_rows"),
        F.max("voltage").alias("max_voltage_check"),
    )
    .filter(F.col("max_voltage_check") > 218.2)
    .drop("max_voltage_check")
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count(F.when(F.col("quality").isin("BAD", "EST"), 1)).alias("reading_quality_count"),
        F.count("*").alias("reading_count"),
        F.count("*").alias("reading_count_2"),
    )
)

with_counts = (
    grouped.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_quality_count"), F.lit(0)).alias("reading_quality_count"),
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        F.coalesce(F.col("m.reading_count_2"), F.lit(0)).alias("reading_count_2"),
    )
)

qualified = (
    with_counts.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.kwh") >= 14.4), "left_semi")
)

result = (
    qualified
    .select(
        "reading_quality",
        "site_id",
        "pct_quality_in_est_bad",
        "stddev_reading_kwh",
    )
    .unpivot(
        ids=["reading_quality", "site_id"],
        values=["pct_quality_in_est_bad", "stddev_reading_kwh"],
        variableColumnName="field",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("reading_quality", "site_id", "field")
)
