from pyspark.sql import functions as F

scoped_readings = (
    df_meter_readings
    .filter((F.col("kwh") <= 80.8) & (F.col("voltage") >= 218.5))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.when(F.col("kwh") >= 87.1, "A").when(F.col("kwh") >= 45.3, "B").otherwise("C").alias("kwh_tier"),
        F.coalesce(F.col("kwh"), F.lit(0.0)).alias("kwh_filled"),
    )
)

joined = (
    scoped_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.quality",
        "s.kwh_tier",
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.zone").alias("site_zone"),
    )
)

with_parent = (
    joined.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.tariff") != "FLAT"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.quality",
        "s.kwh_tier",
        "s.site_installed_kw",
        "s.site_zone",
        F.col("si.installed_kw").alias("site_installed_kw_2"),
    )
)

with_details_site = (
    with_parent.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.quality",
        "s.kwh_tier",
        "s.site_zone",
        F.col("si.installed_kw").alias("site_installed_kw_3"),
    )
)

result = (
    with_details_site
    .rollup("quality", "kwh_tier")
    .agg(
        F.grouping("quality").alias("is_total_quality"),
        F.min("read_at").alias("first_read_at"),
        F.array_sort(F.collect_set("site_zone")).alias("site_zone_list"),
        F.sum(F.when(F.col("site_zone") == "Z4", 1).otherwise(0)).alias("n_site_zone_z4"),
    )
    .select(
        F.coalesce(F.col("quality"), F.lit("ALL")).alias("quality"),
        F.coalesce(F.col("kwh_tier"), F.lit("ALL")).alias("kwh_tier"),
        "is_total_quality",
        "first_read_at",
        "site_zone_list",
        "n_site_zone_z4",
    )
    .orderBy(F.col("quality").asc_nulls_last(), F.col("kwh_tier").asc_nulls_last())
)
