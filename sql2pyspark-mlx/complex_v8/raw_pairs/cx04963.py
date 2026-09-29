from pyspark.sql import functions as F

valid = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.least(F.col("kwh"), F.col("voltage")).alias("least_kwh_voltage"),
        F.to_date(F.col("read_at")).alias("read_at_date"),
        F.abs(F.col("kwh") - 58.5).alias("kwh_distance"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count(F.when(F.col("quality").isin("OK", "EST"), 1)).alias("reading_quality_count"),
        F.max("voltage").alias("max_reading_voltage"),
        F.count(F.when(F.col("quality") == "OK", 1)).alias("reading_quality_count_2"),
    )
)

with_stats = (
    valid.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_quality_count"), F.lit(0)).alias("reading_quality_count"),
        "m.max_reading_voltage",
        F.coalesce(F.col("m.reading_quality_count_2"), F.lit(0)).alias("reading_quality_count_2"),
    )
)

qualified = (
    with_stats.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.kwh") >= 37.0) & (F.col("m.quality").isin("OK", "BAD")), "left_semi")
)

filtered = (
    qualified.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.tariff") == "FLAT") & (F.year(F.col("si.commissioned_on")) == 2024), "left_semi")
)

result = (
    filtered
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        "least_kwh_voltage",
        "read_at_date",
        "kwh_distance",
        "reading_quality_count",
        "max_reading_voltage",
        F.round(F.col("reading_quality_count") / F.nullif(F.col("kwh_distance"), F.lit(0)), 3).alias("reading_quality_count_per_kwh_distance"),
        F.abs(F.col("max_reading_voltage") - 222.2).alias("max_reading_voltage_distance"),
    )
    .orderBy("site_id")
)
