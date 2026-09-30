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

summary = (
    in_window
    .groupBy("site_id", F.year("read_at").alias("read_at_year"))
    .agg(
        F.countDistinct("site_id").alias("distinct_site_ids"),
        F.min("kwh").alias("min_kwh"),
        F.sum(F.when(F.col("zone") == "Z3", 1).otherwise(0)).alias("n_zone_z3"),
        F.min("energy_cost").alias("min_energy_cost"),
        F.round(F.avg("voltage"), 2).alias("avg_voltage"),
    )
)

result = (
    summary
    .filter(F.col("read_at_year") <= 2026)
    .select(
        "site_id",
        "read_at_year",
        "distinct_site_ids",
        "min_kwh",
        "n_zone_z3",
        "min_energy_cost",
        "avg_voltage",
    )
    .orderBy("site_id", F.col("n_zone_z3").desc_nulls_first())
)
