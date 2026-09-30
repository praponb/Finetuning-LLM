from pyspark.sql import functions as F

covered = (
    df_meter_readings.alias("r")
    .join(df_sites.alias("st"), F.col("st.id") == F.col("r.site_id"), "inner")
    .join(
        df_tariff_windows.alias("tw"),
        (F.col("tw.tariff") == F.col("st.tariff"))
        & (F.col("r.read_at") >= F.col("tw.valid_from"))
        & (F.col("r.read_at") < F.col("tw.valid_to")),
        "inner",
    )
    .select(
        "r.id",
        "r.site_id",
        "r.read_at",
        "r.kwh",
        "r.quality",
        "st.zone",
        "tw.rate",
        F.round(F.col("r.kwh") * F.col("tw.rate"), 2).alias("energy_cost"),
    )
)

without_matches = (
    covered.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.quality") != "OK"), "left_anti")
    .filter(F.hour(F.col("read_at")).between(10, 13))
)

joined_site = (
    without_matches.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.installed_kw") >= 93.3)
    .select(
        "s.*",
        F.col("si.zone").alias("site_zone"),
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

rollup_base = (
    joined_site
    .groupBy(
        F.date_trunc("month", "read_at").alias("read_at_month"),
        "quality",
        "zone",
    )
    .agg(
        F.round(F.sum("energy_cost"), 2).alias("total_energy_cost"),
        F.max("read_at").alias("last_read_at"),
        F.array_sort(F.collect_set("quality")).alias("quality_list"),
    )
)

result = (
    rollup_base
    .groupBy("read_at_month")
    .pivot("zone", ["Z4", "Z2", "Z3"])
    .agg(F.max("total_energy_cost"))
    .orderBy("read_at_month")
)
