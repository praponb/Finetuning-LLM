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

site_id_stats = (
    joined_ranges
    .groupBy("site_id")
    .agg(F.max("rate").alias("max_rate"))
)

above_average = (
    joined_ranges.alias("s")
    .join(site_id_stats.alias("g"), F.col("g.site_id") == F.col("s.site_id"), "inner")
    .filter(F.col("s.rate") == F.col("g.max_rate"))
    .select(
        "s.*",
        "g.max_rate",
        F.round(F.col("s.rate") - F.col("g.max_rate"), 2).alias("rate_minus_max"),
    )
)

result = (
    above_average
    .filter(F.col("voltage") < 237.5)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "zone",
        "rate",
        "max_rate",
        "rate_minus_max",
    )
    .orderBy("rate", "zone", "id")
    .limit(5)
)
