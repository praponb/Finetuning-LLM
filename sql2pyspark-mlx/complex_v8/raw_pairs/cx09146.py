from pyspark.sql import functions as F

joined_ranges = (
    df_meter_readings.alias("r")
    .join(df_sites.alias("st"), F.col("st.id") == F.col("r.site_id"), "inner")
    .join(
        df_tariff_windows.alias("tw"),
        (F.col("tw.tariff") == F.col("st.tariff"))
        & (F.col("r.read_at") >= F.col("tw.valid_from"))
        & (F.col("r.read_at") < F.col("tw.valid_to")),
        "left",
    )
    .select(
        "r.id",
        "r.site_id",
        "r.read_at",
        "r.kwh",
        "r.voltage",
        "st.zone",
        "tw.rate",
        F.round(F.col("r.kwh") * F.col("tw.rate"), 2).alias("energy_cost"),
    )
)

combined_details = (
    joined_ranges.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.rate",
        F.col("si.installed_kw").alias("site_installed_kw"),
        "si.site_name",
        F.col("si.zone").alias("site_zone"),
    )
)

site_id_stats = (
    combined_details
    .groupBy("site_id")
    .agg(F.avg("voltage").alias("avg_voltage"))
)

above_average = (
    combined_details.alias("s")
    .join(site_id_stats.alias("g"), F.col("g.site_id") == F.col("s.site_id"), "inner")
    .filter(F.col("s.voltage") > F.col("g.avg_voltage"))
    .select(
        "s.*",
        "g.avg_voltage",
        F.round(F.col("s.voltage") - F.col("g.avg_voltage"), 2).alias("voltage_minus_avg"),
    )
)

result = (
    above_average
    .rollup("site_zone", "site_id")
    .agg(
        F.grouping_id().alias("gid"),
        F.max("read_at").alias("last_read_at"),
        F.sum(F.when(F.col("site_name").isNotNull(), 1).otherwise(0)).alias("n_site_name_is_not_null"),
        F.round(F.stddev("voltage"), 2).alias("stddev_voltage"),
    )
    .select(
        F.coalesce(F.col("site_zone"), F.lit("ALL")).alias("site_zone"),
        "site_id",
        "gid",
        "last_read_at",
        "n_site_name_is_not_null",
        "stddev_voltage",
    )
    .orderBy(F.col("site_zone").asc_nulls_last(), F.col("site_id").asc_nulls_last())
)
