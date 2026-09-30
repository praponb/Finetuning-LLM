from pyspark.sql import functions as F

scoped = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        (F.col("read_at") + F.expr("INTERVAL 1 DAYS")).alias("read_at_plus_1d"),
    )
)

with_details = (
    scoped.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.*",
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.tariff").alias("site_tariff"),
    )
)

result = (
    with_details
    .groupingSets(
        [["site_tariff", "site_id"], ["site_tariff"]],
        "site_tariff", "site_id",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.percentile_approx("kwh", 0.5).alias("median_kwh"),
        F.array_sort(F.collect_set("site_tariff")).alias("site_tariff_list"),
        F.min("read_at_plus_1d").alias("first_read_at_plus_1d"),
    )
    .select(
        F.coalesce(F.col("site_tariff"), F.lit("ALL")).alias("site_tariff"),
        "site_id",
        "gid",
        "median_kwh",
        "site_tariff_list",
        "first_read_at_plus_1d",
    )
)
