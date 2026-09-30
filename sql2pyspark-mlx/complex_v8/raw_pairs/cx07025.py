from pyspark.sql import functions as F

joined_ranges = (
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

tagged = (
    joined_ranges
    .filter(F.col("rate") < 5.7)
    .select(
        "*",
        F.concat_ws(": ", F.col("quality"), F.col("zone")).alias("quality_zone_label"),
        F.initcap(F.trim(F.col("zone"))).alias("zone_clean"),
        F.substring(F.trim(F.col("quality")), 1, 4).alias("quality_prefix"),
    )
)

result = (
    tagged
    .groupBy("site_id", "read_at")
    .pivot("quality", ["OK", "BAD", "EST"])
    .agg(F.avg("kwh"))
    .orderBy("site_id", "read_at")
)
