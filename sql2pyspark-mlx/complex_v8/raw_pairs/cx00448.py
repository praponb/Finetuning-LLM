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

without_matches = (
    in_window.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.quality") == "BAD"), "left_semi")
    .filter(F.col("kwh").between(33.9, 84.9))
)

stats = (
    without_matches
    .filter(F.col("rate") >= 3.7)
    .groupBy("zone", F.to_date("read_at").alias("read_at_day"), "site_id")
    .agg(
        F.round(F.stddev("voltage"), 2).alias("stddev_voltage"),
        F.count("*").alias("n_rows"),
        F.round(F.sum("rate") / F.nullif(F.sum("energy_cost"), F.lit(0)), 3).alias("rate_to_energy_cost_ratio"),
    )
)

with_activity = (
    stats
    .crossJoin(stats.agg(F.avg("rate_to_energy_cost_ratio").alias("overall_avg_rate_to_energy_cost_ratio")))
    .filter(F.col("rate_to_energy_cost_ratio") > F.col("overall_avg_rate_to_energy_cost_ratio"))
    .drop("overall_avg_rate_to_energy_cost_ratio")
)

result = (
    with_activity
    .filter(F.col("stddev_voltage").between(24.5, 183.8))
    .select(
        "zone",
        "read_at_day",
        "site_id",
        "stddev_voltage",
        "n_rows",
        "rate_to_energy_cost_ratio",
        F.substring(F.trim(F.col("zone")), 1, 2).alias("zone_prefix"),
    )
    .orderBy(F.col("rate_to_energy_cost_ratio").desc())
)
