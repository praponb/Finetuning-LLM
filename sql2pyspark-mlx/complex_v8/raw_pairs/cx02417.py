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
        F.count("*").alias("reading_count"),
        F.max("read_at").alias("last_reading_read_at"),
    )
)

with_counts = (
    covered.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        "m.last_reading_read_at",
    )
)

site_id_stats = (
    with_counts
    .groupBy("site_id")
    .agg(F.max("energy_cost").alias("max_energy_cost"))
)

above_average = (
    with_counts.alias("s")
    .join(site_id_stats.alias("g"), F.col("g.site_id") == F.col("s.site_id"), "inner")
    .filter(F.col("s.energy_cost") == F.col("g.max_energy_cost"))
    .select(
        "s.*",
        "g.max_energy_cost",
        F.round(F.col("s.energy_cost") - F.col("g.max_energy_cost"), 2).alias("energy_cost_minus_max"),
    )
)

result = (
    above_average
    .filter(F.col("voltage") < 240.1)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "zone",
        "rate",
        "energy_cost",
        "last_reading_read_at",
        "max_energy_cost",
        "energy_cost_minus_max",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("read_at")).alias("days_since_read_at"),
        F.length(F.trim(F.col("zone"))).alias("zone_len"),
    )
    .orderBy(F.col("energy_cost").desc_nulls_first(), "days_since_read_at", "id")
    .limit(20)
)
