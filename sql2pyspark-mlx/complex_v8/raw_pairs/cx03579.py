from pyspark.sql import functions as F

covered = (
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
        "r.quality",
        "st.zone",
        "tw.rate",
        F.round(F.col("r.kwh") * F.col("tw.rate"), 2).alias("energy_cost"),
    )
)

rollup_base = (
    covered
    .groupBy(F.date_format("read_at", "yyyy-MM").alias("read_at_ym"), "quality")
    .agg(
        F.round(F.sum("rate") / F.nullif(F.sum("kwh"), F.lit(0)), 3).alias("rate_to_kwh_ratio"),
        F.max("energy_cost").alias("max_energy_cost"),
        F.percentile_approx("kwh", 0.5).alias("median_kwh"),
        F.sum(F.when(F.col("zone") == "Z4", 1).otherwise(0)).alias("n_zone_z4"),
        F.max("kwh").alias("max_kwh_check"),
    )
    .filter(F.col("max_kwh_check") > 43.1)
    .drop("max_kwh_check")
)

result = (
    rollup_base
    .select(
        "read_at_ym",
        "quality",
        "rate_to_kwh_ratio",
        "max_energy_cost",
        "median_kwh",
    )
    .orderBy(F.col("rate_to_kwh_ratio").desc_nulls_first())
)
