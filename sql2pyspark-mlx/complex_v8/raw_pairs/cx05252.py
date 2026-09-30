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

scored = (
    joined_ranges
    .filter(F.col("zone").isin("Z2", "Z4"))
    .withColumn("days_since_read_at", F.datediff(F.lit("2025-12-31").cast("date"), F.col("read_at")))
)

qualified = (
    scored.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.read_at") < F.lit("2025-02-18 12:00:00").cast("timestamp")), "left_anti")
)

result = (
    qualified
    .select(
        "id",
        "site_id",
        "kwh",
        "quality",
        "zone",
        "rate",
        "energy_cost",
        "days_since_read_at",
        F.lower(F.regexp_replace(F.trim(F.col("zone")), " ", "_")).alias("zone_slug"),
    )
    .orderBy("zone_slug", "days_since_read_at")
)
