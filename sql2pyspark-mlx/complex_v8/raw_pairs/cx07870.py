from pyspark.sql import functions as F

joined_ranges = (
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
        "r.voltage",
        "st.zone",
        "tw.rate",
        F.round(F.col("r.kwh") * F.col("tw.rate"), 2).alias("energy_cost"),
    )
)

per_group = (
    joined_ranges
    .groupBy("site_id", "zone")
    .agg(
        F.max("read_at").alias("last_read_at"),
        F.max("voltage").alias("max_voltage"),
        F.percentile_approx("kwh", 0.5).alias("median_kwh"),
        F.count("*").alias("n_rows"),
        F.avg("energy_cost").alias("avg_energy_cost_check"),
    )
    .filter(F.col("avg_energy_cost_check") > 90.0)
    .drop("avg_energy_cost_check")
)

with_parent = (
    per_group.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.tariff") == "EV"), "inner")
    .select(
        "s.site_id",
        "s.zone",
        "s.last_read_at",
        "s.max_voltage",
        "s.median_kwh",
        "s.n_rows",
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

exploded_detail_readings = (
    with_parent.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.site_id",
        "s.zone",
        "s.last_read_at",
        "s.median_kwh",
        "s.n_rows",
        F.col("m.id").alias("reading_id"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.kwh").alias("reading_kwh"),
    )
)

result = (
    exploded_detail_readings
    .filter(F.col("n_rows") <= 9)
    .select(
        "site_id",
        "zone",
        "last_read_at",
        "median_kwh",
        "n_rows",
        "reading_id",
        "reading_voltage",
        "reading_quality",
        "reading_kwh",
    )
    .orderBy(F.col("reading_id").asc(), "reading_quality")
)
