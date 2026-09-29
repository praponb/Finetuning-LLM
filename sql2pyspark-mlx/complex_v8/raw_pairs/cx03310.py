from pyspark.sql import functions as F

scoped_readings = (
    df_meter_readings
    .filter((F.col("kwh") >= 45.3) & (F.col("voltage") < 234.7))
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.round(F.col("voltage") / F.nullif(F.col("kwh"), F.lit(0)), 3).alias("voltage_per_kwh"),
        F.upper(F.trim(F.col("quality"))).alias("quality_clean"),
        F.concat(F.col("quality"), F.lit("#"), F.col("id").cast("string")).alias("quality_ref"),
    )
)

with_parent_site = (
    scoped_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.voltage",
        "s.quality",
        "s.quality_ref",
        F.col("si.tariff").alias("site_tariff"),
    )
)

detail_rows_readings = (
    with_parent_site.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.voltage",
        "s.quality",
        "s.site_tariff",
        F.col("m.id").alias("reading_id"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.read_at").alias("reading_read_at"),
    )
)

result = (
    detail_rows_readings
    .rollup("site_tariff", "quality")
    .agg(
        F.count(F.when(F.col("reading_quality") == "OK", 1)).alias("n_reading_quality_ok"),
        F.sum("reading_voltage").alias("total_reading_voltage"),
    )
    .orderBy(
        F.col("site_tariff").asc_nulls_last(),
        F.col("quality").asc_nulls_last(),
    )
)
