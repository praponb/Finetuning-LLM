from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id")
w_all_by_kwh_moving_6 = Window.orderBy(F.col("kwh").desc(), "id").rowsBetween(-6, Window.currentRow)

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

with_trends = (
    joined_ranges
    .withColumn("site_id_sum_energy_cost", F.sum("energy_cost").over(w_site_id))
    .withColumn("rate_max_7", F.max("rate").over(w_all_by_kwh_moving_6))
)

with_parent = (
    with_trends.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (~F.col("si.tariff").isin("FLAT", "TOU")), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.rate",
        "s.energy_cost",
        "s.site_id_sum_energy_cost",
        "s.rate_max_7",
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

with_details_site = (
    with_parent.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.voltage",
        "s.rate",
        "s.site_id_sum_energy_cost",
        "s.rate_max_7",
        "si.site_name",
        F.col("si.installed_kw").alias("site_installed_kw_2"),
        F.col("si.tariff").alias("site_tariff"),
    )
)

result = (
    with_details_site
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "site_name",
        "site_installed_kw_2",
    )
    .orderBy("site_installed_kw_2", F.col("site_name").desc_nulls_first())
)
