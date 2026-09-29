from pyspark.sql import functions as F

in_window = (
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
    in_window
    .filter(F.col("zone") != "Z2")
    .select(
        "*",
        F.initcap(F.trim(F.col("zone"))).alias("zone_clean"),
        F.abs(F.col("rate") - 4.2).alias("rate_distance"),
        (F.floor(F.col("kwh") / 25) * 25).alias("kwh_bucket"),
    )
)

enriched = (
    scored
    .filter(F.col("rate") < 7.6)
    .select(
        "*",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("read_at")).alias("days_since_read_at"),
        F.date_format(F.col("read_at"), "yyyy-MM-dd").alias("read_at_label"),
    )
)

result = (
    enriched
    .filter(F.col("kwh_bucket").between(18, 84))
    .select(
        "id",
        "site_id",
        "kwh",
        "quality",
        "zone",
        "rate",
        "kwh_bucket",
        "days_since_read_at",
        (F.col("read_at") + F.expr("INTERVAL 7 DAYS")).alias("read_at_plus_7d"),
    )
    .orderBy(F.col("zone").desc(), F.col("kwh_bucket").desc())
)
