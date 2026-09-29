from pyspark.sql import functions as F

cleaned_readings = (
    df_meter_readings
    .filter((F.col("site_id").isNotNull()) & (F.col("kwh") >= 50.4))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.round(F.col("kwh") / F.nullif(F.col("voltage"), F.lit(0)), 3).alias("kwh_per_voltage"),
    )
)

derived = (
    cleaned_readings
    .withColumn("quality_group", F.when(F.col("quality").isin("OK", "BAD"), "group_a").when(F.col("quality") == "EST", "group_b").otherwise("other"))
)

exploded_detail_readings = (
    derived.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.voltage",
        "s.quality",
        "s.kwh_per_voltage",
        "s.quality_group",
        F.col("m.id").alias("reading_id"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.kwh").alias("reading_kwh"),
    )
)

reading_quality_stats = (
    exploded_detail_readings
    .groupBy("reading_quality")
    .agg(F.max("kwh").alias("max_kwh"))
)

above_average = (
    exploded_detail_readings.alias("s")
    .join(reading_quality_stats.alias("g"), F.col("g.reading_quality") == F.col("s.reading_quality"), "inner")
    .filter(F.col("s.kwh") == F.col("g.max_kwh"))
    .select(
        "s.*",
        "g.max_kwh",
        F.round(F.col("s.kwh") - F.col("g.max_kwh"), 2).alias("kwh_minus_max"),
    )
)

with_details_site = (
    above_average.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.voltage",
        "s.quality",
        "s.kwh_per_voltage",
        "s.quality_group",
        "s.reading_id",
        "s.reading_quality",
        "s.kwh_minus_max",
        F.col("si.zone").alias("site_zone"),
        "si.site_name",
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

derived_2 = (
    with_details_site
    .select(
        "id",
        "site_id",
        "voltage",
        "quality",
        "quality_group",
        "reading_id",
        "reading_quality",
        "kwh_minus_max",
        "site_name",
        (F.col("kwh_per_voltage") + F.col("kwh_minus_max")).alias("kwh_per_voltage_plus_kwh_minus_max"),
        F.concat_ws("-", F.col("quality_group"), F.col("site_zone")).alias("quality_group_site_zone_label"),
    )
)

result = (
    derived_2
    .groupBy("quality")
    .pivot("reading_quality", ["OK", "BAD", "EST"])
    .agg(F.avg("kwh_per_voltage_plus_kwh_minus_max").alias("avg_v"), F.max("kwh_per_voltage_plus_kwh_minus_max").alias("max_v"))
    .orderBy("quality")
)
