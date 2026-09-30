from pyspark.sql import functions as F

in_window = (
    df_meter_readings.alias("a")
    .join(
        df_meter_readings.alias("b"),
        (F.col("b.site_id") == F.col("a.site_id"))
        & (F.col("b.read_at") < F.col("a.read_at"))
        & (F.col("b.read_at") >= F.col("a.read_at") - F.expr("INTERVAL 1 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.site_id", "a.read_at")
    .agg(
        F.count("b.id").alias("prior_rows_1d"),
        F.sum("b.kwh").alias("prior_kwh_1d"),
    )
)

with_details = (
    in_window.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.prior_rows_1d",
        "s.prior_kwh_1d",
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.tariff").alias("site_tariff"),
        "si.site_name",
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.sum("voltage").alias("total_reading_voltage"),
        F.count("*").alias("reading_count"),
    )
)

profiled = (
    with_details.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "m.total_reading_voltage",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
    )
)

labeled = (
    profiled
    .filter(F.col("site_installed_kw") < 284.0)
    .select(
        "id",
        "site_id",
        "prior_kwh_1d",
        "site_tariff",
        "total_reading_voltage",
        "reading_count",
        F.when(F.col("site_tariff").isin("FLAT", "EV"), "group_a").when(F.col("site_tariff") == "TOU", "group_b").otherwise("other").alias("site_tariff_group"),
        F.when(F.col("site_tariff").isin("TOU", "EV"), "group_a").when(F.col("site_tariff") == "FLAT", "group_b").otherwise("other").alias("site_tariff_group_2"),
    )
)

result = (
    labeled
    .select(
        "id",
        "site_id",
        "prior_kwh_1d",
        "site_tariff",
        "reading_count",
        "site_tariff_group",
        "site_tariff_group_2",
        (F.col("reading_count") - F.col("prior_kwh_1d")).alias("reading_count_minus_prior_kwh_1d"),
        F.concat(F.col("site_tariff"), F.lit("#"), F.col("site_id").cast("string")).alias("site_tariff_ref"),
    )
)
