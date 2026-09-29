from pyspark.sql import functions as F

matched = (
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

screened = (
    matched
    .crossJoin(matched.agg(F.avg("kwh").alias("overall_avg_kwh")))
    .filter(F.col("kwh") > F.col("overall_avg_kwh"))
    .drop("overall_avg_kwh")
)

result = (
    screened
    .groupBy("site_id")
    .pivot("zone", ["Z4", "Z3"])
    .agg(F.sum("energy_cost").alias("total"), F.max("energy_cost").alias("max_v"))
)
