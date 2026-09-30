from pyspark.sql import functions as F

valid = (
    df_meter_readings
    .filter((F.month(F.col("read_at")).isin(1, 2)) | (F.col("voltage") >= 223.3))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
    )
)

with_activity = (
    valid.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.quality") == "BAD"), "left_semi")
    .filter(F.col("voltage").between(220.2, 243.7))
)

joined_site = (
    with_activity.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "si.site_name",
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

scored = (
    joined_site
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "site_name",
        "site_installed_kw",
        F.coalesce(F.col("voltage"), F.lit(0.0)).alias("voltage_filled"),
        (F.col("site_installed_kw") + F.col("voltage")).alias("site_installed_kw_plus_voltage"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.min("voltage").alias("min_reading_voltage"),
        F.max("read_at").alias("last_reading_read_at"),
    )
)

with_stats = (
    scored.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.min_reading_voltage", "m.last_reading_read_at")
)

enriched = (
    with_stats
    .select(
        "id",
        "read_at",
        "site_name",
        "site_installed_kw",
        "voltage_filled",
        "site_installed_kw_plus_voltage",
        "min_reading_voltage",
        "last_reading_read_at",
        F.substring(F.trim(F.col("site_name")), 1, 2).alias("site_name_prefix"),
        F.substring(F.trim(F.col("site_name")), 1, 4).alias("site_name_prefix_2"),
        F.round(F.col("voltage_filled") * 1.07, 2).alias("voltage_filled_adj"),
    )
)

totals = (
    enriched
    .filter(F.col("site_installed_kw_plus_voltage") > 190.0)
    .groupBy(
        F.date_format("read_at", "yyyy-MM").alias("read_at_ym"),
        F.date_trunc("month", "last_reading_read_at").alias("last_reading_read_at_month"),
    )
    .agg(
        F.percentile_approx("voltage_filled_adj", 0.5).alias("median_voltage_filled_adj"),
        F.count("*").alias("cnt"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum("voltage_filled").alias("total_voltage_filled"),
        F.round(F.stddev("site_installed_kw"), 2).alias("stddev_site_installed_kw"),
        F.count(F.when(F.col("site_name").isNotNull(), 1)).alias("n_site_name_is_not_null"),
        F.max("site_installed_kw").alias("max_site_installed_kw_check"),
    )
    .filter(F.col("max_site_installed_kw_check") > 141.6)
    .drop("max_site_installed_kw_check")
)

result = (
    totals
    .cube("n_site_name_is_not_null", "cnt")
    .agg(F.grouping_id().alias("grouping_level"), F.min("cnt").alias("min_cnt"))
    .orderBy(
        F.col("n_site_name_is_not_null").asc_nulls_last(),
        F.col("cnt").asc_nulls_last(),
    )
)
