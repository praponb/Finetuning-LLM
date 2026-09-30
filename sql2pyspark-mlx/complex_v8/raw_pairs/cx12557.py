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
        "r.quality",
        "st.zone",
        "tw.rate",
        F.round(F.col("r.kwh") * F.col("tw.rate"), 2).alias("energy_cost"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.min("read_at").alias("first_reading_read_at"))
)

profiled = (
    joined_ranges.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.first_reading_read_at")
)

with_details_site = (
    profiled.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.tariff") != "TOU"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.quality",
        "s.zone",
        "s.energy_cost",
        "s.first_reading_read_at",
        F.col("si.tariff").alias("site_tariff"),
    )
)

result = (
    with_details_site
    .cube("zone", "quality", "site_tariff")
    .agg(
        F.grouping("zone").alias("is_total_zone"),
        F.max("first_reading_read_at").alias("last_first_reading_read_at"),
        F.round(F.avg("energy_cost"), 2).alias("avg_energy_cost"),
    )
    .select(
        F.coalesce(F.col("zone"), F.lit("ALL")).alias("zone"),
        F.coalesce(F.col("quality"), F.lit("ALL")).alias("quality"),
        F.coalesce(F.col("site_tariff"), F.lit("ALL")).alias("site_tariff"),
        "is_total_zone",
        "last_first_reading_read_at",
        "avg_energy_cost",
    )
)
