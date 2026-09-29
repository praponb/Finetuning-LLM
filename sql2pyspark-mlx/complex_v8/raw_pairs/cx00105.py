from pyspark.sql import functions as F

valid = (
    df_meter_readings
    .filter((F.hour(F.col("read_at")).between(7, 9)) & (F.col("kwh") <= 89.8))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.coalesce(F.col("voltage"), F.lit(0.0)).alias("voltage_filled"),
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
    )
)

with_details = (
    valid.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.installed_kw") > 141.6), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.voltage",
        "s.voltage_filled",
        "s.quality_len",
        F.col("si.zone").alias("site_zone"),
        F.col("si.installed_kw").alias("site_installed_kw"),
        "si.site_name",
    )
)

filtered = (
    with_details.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.quality") == "EST"), "left_anti")
    .filter(F.col("site_installed_kw").between(96.9, 386.6))
)

site_zone_stats = (
    filtered
    .groupBy("site_zone")
    .agg(F.avg("voltage_filled").alias("avg_voltage_filled"))
)

above_average = (
    filtered.alias("s")
    .join(site_zone_stats.alias("g"), F.col("g.site_zone") == F.col("s.site_zone"), "inner")
    .filter(F.col("s.voltage_filled") > F.col("g.avg_voltage_filled"))
    .select(
        "s.*",
        "g.avg_voltage_filled",
        F.round(F.col("s.voltage_filled") - F.col("g.avg_voltage_filled"), 2).alias("voltage_filled_minus_avg"),
    )
)

result = (
    above_average
    .groupingSets(
        [["site_id", "site_zone", "quality_len"], ["site_id"], []],
        "site_id", "site_zone", "quality_len",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.array_sort(F.collect_set("site_name")).alias("site_name_list"),
        F.count("*").alias("row_count"),
        F.max("voltage").alias("max_voltage"),
    )
    .orderBy(
        F.col("site_id").asc_nulls_last(),
        F.col("site_zone").asc_nulls_last(),
        F.col("quality_len").asc_nulls_last(),
    )
)
