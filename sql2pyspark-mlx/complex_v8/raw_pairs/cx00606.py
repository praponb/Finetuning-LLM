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
        "r.voltage",
        "st.zone",
        "tw.rate",
        F.round(F.col("r.kwh") * F.col("tw.rate"), 2).alias("energy_cost"),
    )
)

with_details_site = (
    in_window.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.commissioned_on") >= F.lit("2024-06-27").cast("date")), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.rate",
        "s.energy_cost",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

with_activity = (
    with_details_site.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.voltage") >= 216.1) & (F.col("m.kwh").between(35.0, 106.0)), "left_anti")
    .filter(F.col("rate") <= 7.6)
)

result = (
    with_activity
    .select(
        "id",
        "site_id",
        "kwh",
        "rate",
        "site_commissioned_on",
    )
    .orderBy("rate", F.col("site_commissioned_on").desc())
)
