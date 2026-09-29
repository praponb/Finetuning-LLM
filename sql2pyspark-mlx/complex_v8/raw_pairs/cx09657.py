from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_site_zone = Window.partitionBy("site_id", "site_zone")
w_site_zone_site_id_by_energy = Window.partitionBy("site_zone", "site_id").orderBy(F.col("energy_cost").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

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

with_details_site = (
    joined_ranges.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.zone",
        "s.rate",
        "s.energy_cost",
        F.col("si.zone").alias("site_zone"),
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

ranked = (
    with_details_site
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "zone",
        "rate",
        "site_zone",
        "site_installed_kw",
        F.round(F.col("site_installed_kw") / F.nullif(F.sum("site_installed_kw").over(w_site_id_site_zone), F.lit(0)), 4).alias("site_installed_kw_share_of_site_id_site_zone"),
        F.last("rate").over(w_site_zone_site_id_by_energy).alias("last_rate"),
    )
)

with_parent = (
    ranked.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select("s.*", F.col("si.tariff").alias("site_tariff"))
)

result = (
    with_parent
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "zone",
        "site_zone",
        "site_installed_kw",
        "site_installed_kw_share_of_site_id_site_zone",
        "last_rate",
        "site_tariff",
        F.lower(F.regexp_replace(F.trim(F.col("zone")), " ", "_")).alias("zone_slug"),
        F.length(F.trim(F.col("site_tariff"))).alias("site_tariff_len"),
    )
    .orderBy(F.col("site_tariff").asc())
)
