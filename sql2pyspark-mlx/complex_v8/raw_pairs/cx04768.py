from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_zone_site_id_by_voltage = Window.partitionBy("zone", "site_id").orderBy(F.col("voltage").asc_nulls_last(), "id")
w_site_id = Window.partitionBy("site_id")

matched = (
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
        "r.voltage",
        "st.zone",
        "tw.rate",
        F.round(F.col("r.kwh") * F.col("tw.rate"), 2).alias("energy_cost"),
    )
)

metrics = (
    matched
    .select(
        "id",
        "site_id",
        "zone",
        "energy_cost",
        (F.col("rate") - F.lag("rate").over(w_zone_site_id_by_voltage)).alias("rate_change"),
        F.round(F.col("rate") - F.avg("rate").over(w_site_id), 2).alias("rate_vs_site_id_avg"),
        F.lag("rate").over(w_zone_site_id_by_voltage).alias("prev_rate"),
    )
)

result = (
    metrics
    .filter(F.col("energy_cost").between(135.0, 630.0))
    .select(
        "id",
        "site_id",
        "zone",
        "energy_cost",
        "rate_change",
        "rate_vs_site_id_avg",
        (F.col("rate_change") - F.col("energy_cost")).alias("rate_change_minus_energy_cost"),
    )
    .orderBy("rate_change")
)
