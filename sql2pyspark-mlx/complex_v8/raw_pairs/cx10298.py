from pyspark.sql import functions as F

in_window = (
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
    in_window
    .groupBy("zone")
    .agg(F.percentile_approx("kwh", 0.5).alias("median_kwh"))
)

outliers = (
    in_window.alias("s")
    .join(zone_stats.alias("g"), F.col("g.zone") == F.col("s.zone"), "inner")
    .filter(F.col("s.kwh") >= F.col("g.median_kwh"))
    .select(
        "s.*",
        "g.median_kwh",
        F.round(F.col("s.kwh") - F.col("g.median_kwh"), 2).alias("kwh_minus_median"),
    )
)

rollup_base = (
    outliers
    .filter(F.col("kwh_minus_median") <= 96.0)
    .groupBy(F.to_date("read_at").alias("read_at_day"), "site_id", "zone")
    .agg(
        F.round(F.sum("kwh_minus_median") / F.nullif(F.sum("kwh"), F.lit(0)), 3).alias("kwh_minus_median_to_kwh_ratio"),
        F.max("kwh").alias("max_kwh"),
        F.round(F.sum("voltage") / F.nullif(F.sum("kwh"), F.lit(0)), 3).alias("voltage_to_kwh_ratio"),
        F.max("read_at").alias("last_read_at"),
        F.min("kwh").alias("min_kwh"),
        F.sum(F.when(F.col("zone").isin("Z4", "Z2"), F.col("voltage"))).alias("voltage_zone_in_z4_z2"),
        F.max("kwh_minus_median").alias("max_kwh_minus_median_check"),
    )
    .filter(F.col("max_kwh_minus_median_check") > -60.0)
    .drop("max_kwh_minus_median_check")
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.round(F.avg("voltage"), 2).alias("avg_reading_voltage"),
        F.max("read_at").alias("last_reading_read_at"),
    )
)

with_counts = (
    rollup_base.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.avg_reading_voltage", "m.last_reading_read_at")
)

result = (
    with_counts
    .select(
        "read_at_day",
        "site_id",
        "kwh_minus_median_to_kwh_ratio",
        "max_kwh",
        "voltage_to_kwh_ratio",
        "last_read_at",
        "min_kwh",
        "avg_reading_voltage",
        "last_reading_read_at",
        F.length(F.trim(F.col("zone"))).alias("zone_len"),
        F.greatest(F.col("max_kwh"), F.col("voltage_to_kwh_ratio")).alias("greatest_max_kwh_voltage_to_kwh_ratio"),
    )
    .orderBy("site_id")
)
