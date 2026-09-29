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

with_activity = (
    covered.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left_semi")
)

with_activity_2 = (
    with_activity
    .crossJoin(with_activity.agg(F.min("kwh").alias("overall_min_kwh")))
    .filter(F.col("kwh") > F.col("overall_min_kwh"))
    .drop("overall_min_kwh")
)

combined_details = (
    with_activity_2.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.tariff").alias("site_tariff"),
    )
)

result = (
    combined_details
    .select(
        "id",
        "site_id",
        "read_at",
        "zone",
        "rate",
        "site_installed_kw",
        "site_tariff",
    )
    .orderBy(F.col("zone").asc_nulls_last(), "site_id")
)
