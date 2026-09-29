from pyspark.sql import functions as F

filtered_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("read_at")).alias("days_since_read_at"),
        F.concat(F.col("quality"), F.lit("#"), F.col("site_id").cast("string")).alias("quality_ref"),
        F.round(F.col("kwh") / F.nullif(F.col("voltage"), F.lit(0)), 3).alias("kwh_per_voltage"),
    )
)

detail_rows_readings = (
    filtered_readings.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.days_since_read_at",
        F.col("m.id").alias("reading_id"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.quality").alias("reading_quality"),
    )
)

with_details_site = (
    detail_rows_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.days_since_read_at",
        "s.reading_id",
        "s.reading_read_at",
        "s.reading_quality",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        "si.site_name",
        F.col("si.tariff").alias("site_tariff"),
    )
)

enriched = (
    with_details_site.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.col("si.zone").alias("site_zone"),
        F.col("si.tariff").alias("site_tariff_2"),
    )
)

result = (
    enriched
    .groupingSets(
        [["site_id", "reading_id"], ["site_id"], ["reading_id"]],
        "site_id", "reading_id",
    )
    .agg(
        F.grouping("site_id").alias("is_total_site_id"),
        F.count(F.when(F.col("reading_quality").isin("EST", "BAD"), 1)).alias("n_reading_quality_in_est_bad"),
        F.round(100 * F.count(F.when(~F.col("reading_quality").isin("EST", "BAD"), 1)) / F.count("*"), 1).alias("pct_reading_quality_not_in_est_b"),
    )
    .orderBy(F.col("site_id").asc_nulls_last(), F.col("reading_id").asc_nulls_last())
)
