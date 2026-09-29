from pyspark.sql import functions as F

matched = (
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

with_parent_site = (
    matched.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.zone",
        "s.rate",
        "s.energy_cost",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.max("voltage").alias("max_reading_voltage"),
        F.count(F.when(F.col("quality").isin("EST", "OK"), 1)).alias("reading_quality_count"),
    )
)

summarized_rows = (
    with_parent_site.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "m.max_reading_voltage",
        F.coalesce(F.col("m.reading_quality_count"), F.lit(0)).alias("reading_quality_count"),
    )
)

with_activity = (
    summarized_rows.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.installed_kw").between(96.9, 373.3)) & (F.col("si.tariff").isin("TOU", "EV")), "left_anti")
)

result = (
    with_activity
    .select(
        "id",
        "site_id",
        "energy_cost",
        "site_commissioned_on",
        "max_reading_voltage",
        "reading_quality_count",
        (F.col("max_reading_voltage") + F.col("reading_quality_count")).alias("max_reading_voltage_plus_reading_quality_count"),
    )
    .orderBy(
        "energy_cost",
        F.col("max_reading_voltage_plus_reading_quality_count").asc_nulls_last(),
        "id",
    )
    .limit(10)
)
