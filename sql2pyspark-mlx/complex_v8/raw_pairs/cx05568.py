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

joined_site = (
    joined_ranges.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select("s.*", F.col("si.zone").alias("site_zone"), "si.site_name")
)

summary = (
    joined_site
    .filter(F.col("zone").isin("Z4", "Z2"))
    .groupBy("site_zone", "zone")
    .agg(
        F.min("read_at").alias("first_read_at"),
        F.count(F.when(F.col("zone") == "Z3", 1)).alias("n_zone_z3"),
    )
)

filtered = (
    summary
    .crossJoin(summary.agg(F.avg("n_zone_z3").alias("overall_avg_n_zone_z3")))
    .filter(F.col("n_zone_z3") > F.col("overall_avg_n_zone_z3"))
    .drop("overall_avg_n_zone_z3")
)

result = (
    filtered
    .select(
        "site_zone",
        "zone",
        "first_read_at",
        "n_zone_z3",
    )
)
