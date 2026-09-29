from pyspark.sql import functions as F

valid_readings = (
    df_meter_readings
    .filter(F.col("voltage") > 216.5)
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.greatest(F.col("kwh"), F.col("voltage")).alias("greatest_kwh_voltage"),
        F.abs(F.col("kwh") - 42.1).alias("kwh_distance"),
    )
)

with_parent = (
    valid_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.*",
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.tariff").alias("site_tariff"),
    )
)

with_details = (
    with_parent.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.voltage",
        "s.quality",
        "s.kwh_distance",
        "s.site_tariff",
        "si.site_name",
    )
)

without_matches = (
    with_details.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.kwh") < 87.1), "left_anti")
)

screened = (
    without_matches.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.quality").isNotNull()), "left_semi")
)

with_details_site = (
    screened.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.installed_kw") >= 96.9)
    .select(
        "s.id",
        "s.site_id",
        "s.voltage",
        "s.quality",
        "s.kwh_distance",
        "s.site_tariff",
        "s.site_name",
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

result = (
    with_details_site
    .cube("site_tariff", "site_id", "quality")
    .agg(
        F.count(F.when(F.col("site_name").isNotNull(), 1)).alias("n_site_name_is_not_null"),
        F.count(F.when(~F.col("site_tariff").isin("TOU", "FLAT"), 1)).alias("n_site_tariff_not_in_tou_flat"),
        F.array_sort(F.collect_set("quality")).alias("quality_list"),
    )
    .select(
        F.coalesce(F.col("site_tariff"), F.lit("ALL")).alias("site_tariff"),
        "site_id",
        F.coalesce(F.col("quality"), F.lit("ALL")).alias("quality"),
        "n_site_name_is_not_null",
        "n_site_tariff_not_in_tou_flat",
        "quality_list",
    )
)
