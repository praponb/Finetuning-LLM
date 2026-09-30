from pyspark.sql import functions as F

base = (
    df_meter_readings
    .filter(F.col("kwh") < 109.3)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.substring(F.trim(F.col("quality")), 1, 2).alias("quality_prefix"),
        F.round(F.col("kwh") / F.nullif(F.col("voltage"), F.lit(0)), 3).alias("kwh_per_voltage"),
    )
)

with_details_site = (
    base.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.quality",
        "s.quality_prefix",
        "s.kwh_per_voltage",
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        "si.site_name",
    )
)

combined_details_site = (
    with_details_site.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select("s.*", F.col("si.zone").alias("site_zone"))
)

result = (
    combined_details_site
    .groupingSets(
        [["site_zone", "site_id"], ["site_zone"], []],
        "site_zone", "site_id",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.count(F.when(F.col("quality").isNotNull(), 1)).alias("n_quality_is_not_null"),
        F.countDistinct("site_zone").alias("distinct_site_zones"),
        F.round(F.stddev("kwh_per_voltage"), 2).alias("stddev_kwh_per_voltage"),
    )
    .orderBy(F.col("site_zone").asc_nulls_last(), F.col("site_id").asc_nulls_last())
)
