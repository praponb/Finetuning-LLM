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

totals = (
    covered
    .groupBy("zone", "site_id", F.to_date("read_at").alias("read_at_day"))
    .agg(
        F.sum("energy_cost").alias("total_energy_cost"),
        F.count("*").alias("n_rows"),
        F.countDistinct("id").alias("distinct_ids"),
        F.avg("voltage").alias("avg_voltage_check"),
    )
    .filter(F.col("avg_voltage_check") > 215.7)
    .drop("avg_voltage_check")
)

screened = (
    totals.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.to_date(F.col("m.read_at")) < F.lit("2025-02-06").cast("date")) & (~F.col("m.quality").isin("BAD", "EST")), "left_semi")
    .filter(F.col("total_energy_cost") < 4320.0)
)

result = (
    screened
    .groupBy("read_at_day")
    .pivot("zone", ["Z3", "Z4"])
    .agg(F.sum("total_energy_cost"))
    .orderBy("read_at_day")
)
