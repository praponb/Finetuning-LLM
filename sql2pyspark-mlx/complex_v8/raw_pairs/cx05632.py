from pyspark.sql import functions as F

prepared_readings = (
    df_meter_readings
    .filter(F.col("kwh") < 82.9)
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
        F.least(F.col("voltage"), F.col("kwh")).alias("least_voltage_kwh"),
    )
)

with_details = (
    prepared_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.voltage",
        "s.quality_slug",
        "s.least_voltage_kwh",
        "si.site_name",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

labeled = (
    with_details
    .withColumn("kwh_per_least_voltage_kwh", F.round(F.col("kwh") / F.nullif(F.col("least_voltage_kwh"), F.lit(0)), 3))
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.round(F.avg("kwh"), 2).alias("avg_reading_kwh"),
        F.max("read_at").alias("last_reading_read_at"),
        F.max("voltage").alias("max_reading_voltage"),
    )
)

with_stats = (
    labeled.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "m.avg_reading_kwh",
        "m.last_reading_read_at",
        "m.max_reading_voltage",
    )
)

result = (
    with_stats
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "least_voltage_kwh",
        "site_name",
        "site_commissioned_on",
        "site_installed_kw",
        "kwh_per_least_voltage_kwh",
        "avg_reading_kwh",
        "last_reading_read_at",
        "max_reading_voltage",
        F.substring(F.trim(F.col("site_name")), 1, 3).alias("site_name_prefix"),
        F.abs(F.col("avg_reading_kwh") - 42.0).alias("avg_reading_kwh_distance"),
    )
    .orderBy(F.col("last_reading_read_at").asc_nulls_last(), "voltage")
)
