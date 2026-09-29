from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_tariff = Window.partitionBy("site_tariff")
w_site_commissioned_on_month_s = Window.partitionBy(F.date_trunc("month", "site_commissioned_on"), "site_tariff").orderBy("energy_cost", "id").rowsBetween(-2, -1)

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
        "r.voltage",
        "st.zone",
        "tw.rate",
        F.round(F.col("r.kwh") * F.col("tw.rate"), 2).alias("energy_cost"),
    )
)

enriched = (
    covered.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select("s.*", F.col("si.zone").alias("site_zone"), "si.site_name")
)

enriched_2 = (
    enriched.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.rate",
        "s.energy_cost",
        "s.site_zone",
        "s.site_name",
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.site_name").alias("site_site_name"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

ranked = (
    enriched_2
    .select(
        "*",
        F.round(F.col("energy_cost") / F.nullif(F.sum("energy_cost").over(w_site_tariff), F.lit(0)), 4).alias("energy_cost_share_of_site_tariff"),
        F.max("rate").over(w_site_commissioned_on_month_s).alias("rate_max_2_prior"),
    )
)

result = (
    ranked
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "rate",
        "energy_cost",
        "site_zone",
        "site_tariff",
        "site_site_name",
        "site_commissioned_on",
        "energy_cost_share_of_site_tariff",
        "rate_max_2_prior",
        F.abs(F.col("rate_max_2_prior") - 16.3).alias("rate_max_2_prior_distance"),
    )
    .orderBy("rate_max_2_prior")
)
