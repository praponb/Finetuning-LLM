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

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.round(F.avg("voltage"), 2).alias("avg_reading_voltage"),
        F.min("read_at").alias("first_reading_read_at"),
    )
)

with_stats = (
    joined_ranges.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.avg_reading_voltage", "m.first_reading_read_at")
)

site_id_stats = (
    with_stats
    .groupBy("site_id")
    .agg(F.percentile_approx("rate", 0.5).alias("median_rate"))
)

vs_group = (
    with_stats.alias("s")
    .join(site_id_stats.alias("g"), F.col("g.site_id") == F.col("s.site_id"), "inner")
    .filter(F.col("s.rate") >= F.col("g.median_rate"))
    .select(
        "s.*",
        "g.median_rate",
        F.round(F.col("s.rate") - F.col("g.median_rate"), 2).alias("rate_minus_median"),
    )
)

result = (
    vs_group
    .filter(F.col("first_reading_read_at") < F.lit("2025-02-05 09:00:00").cast("timestamp"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "zone",
        "rate",
        "energy_cost",
        "avg_reading_voltage",
        "rate_minus_median",
        F.substring(F.trim(F.col("zone")), 1, 2).alias("zone_prefix"),
        F.least(F.col("voltage"), F.col("median_rate")).alias("least_voltage_median_rate"),
    )
    .orderBy(F.col("kwh").asc())
)
