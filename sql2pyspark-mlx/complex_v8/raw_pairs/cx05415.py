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

filtered = (
    joined_ranges.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.voltage") <= 233.1), "left_anti")
    .filter(F.col("read_at") >= F.lit("2025-01-19 07:00:00").cast("timestamp"))
)

without_matches = (
    filtered.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.voltage") >= 221.8), "left_semi")
    .filter(F.col("site_id").isNotNull())
)

result = (
    without_matches
    .filter(F.col("read_at") >= F.lit("2025-01-25 13:00:00").cast("timestamp"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "zone",
        "rate",
        "energy_cost",
        F.date_format(F.col("read_at"), "yyyy-MM").alias("read_at_label"),
        F.coalesce(F.col("kwh"), F.lit(0.0)).alias("kwh_filled"),
    )
    .orderBy(F.col("kwh").desc_nulls_first(), "id")
    .limit(10)
)
