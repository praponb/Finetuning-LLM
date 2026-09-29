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
        "r.voltage",
        "st.zone",
        "tw.rate",
        F.round(F.col("r.kwh") * F.col("tw.rate"), 2).alias("energy_cost"),
    )
)

combined_details = (
    joined_ranges.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.commissioned_on") >= F.lit("2024-08-24").cast("date")), "inner")
    .filter(F.col("si.site_name").isNotNull())
    .select(
        "s.*",
        F.col("si.zone").alias("site_zone"),
        F.col("si.installed_kw").alias("site_installed_kw"),
        "si.site_name",
    )
)

result = (
    combined_details
    .filter(F.upper(F.trim(F.col("site_name"))).like("S%"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "rate",
        "energy_cost",
        "site_installed_kw",
        "site_name",
        (F.floor(F.col("site_installed_kw") / 100) * 100).alias("site_installed_kw_bucket"),
        F.lower(F.regexp_replace(F.trim(F.col("site_zone")), " ", "_")).alias("site_zone_slug"),
    )
    .orderBy("rate")
)
