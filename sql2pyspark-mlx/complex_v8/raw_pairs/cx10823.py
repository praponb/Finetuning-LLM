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

zone_stats = (
    joined_ranges
    .groupBy("zone")
    .agg(F.max("rate").alias("max_rate"))
)

compared = (
    joined_ranges.alias("s")
    .join(zone_stats.alias("g"), F.col("g.zone") == F.col("s.zone"), "inner")
    .filter(F.col("s.rate") == F.col("g.max_rate"))
    .select(
        "s.*",
        "g.max_rate",
        F.round(F.col("s.rate") - F.col("g.max_rate"), 2).alias("rate_minus_max"),
    )
)

site_id_stats = (
    compared
    .groupBy("site_id")
    .agg(F.max("energy_cost").alias("max_energy_cost"))
)

outliers = (
    compared.alias("s")
    .join(site_id_stats.alias("g"), F.col("g.site_id") == F.col("s.site_id"), "inner")
    .filter(F.col("s.energy_cost") == F.col("g.max_energy_cost"))
    .select(
        "s.*",
        "g.max_energy_cost",
        F.round(F.col("s.energy_cost") - F.col("g.max_energy_cost"), 2).alias("energy_cost_minus_max"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.min("read_at").alias("first_reading_read_at"))
)

with_counts = (
    outliers.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.first_reading_read_at")
)

result = (
    with_counts
    .select(
        "id",
        "kwh",
        "zone",
        "rate",
        "energy_cost",
        "max_energy_cost",
        "energy_cost_minus_max",
        "first_reading_read_at",
    )
    .orderBy(F.col("rate").desc(), F.col("energy_cost_minus_max").asc_nulls_last())
)
