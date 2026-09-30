from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id")
w_all_by_voltage = Window.orderBy("voltage", "id")
w_site_id_by_voltage_prior_2 = Window.partitionBy("site_id").orderBy("voltage", "id").rowsBetween(-2, -1)
w_read_at_month_zone = Window.partitionBy(F.month("read_at"), "zone")
w_site_id_by_voltage_ties = Window.partitionBy("site_id").orderBy("voltage")
w_site_id_by_voltage_running = Window.partitionBy("site_id").orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

covered = (
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

with_windows = (
    covered
    .select(
        "*",
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_site_id), F.lit(0)), 4).alias("kwh_share_of_site_id"),
        F.round(F.cume_dist().over(w_all_by_voltage), 3).alias("voltage_cume_dist"),
    )
)

labeled = (
    with_windows
    .filter(F.col("rate") >= 4.2)
    .select(
        "*",
        F.abs(F.col("kwh_share_of_site_id") - 0.6).alias("kwh_share_of_site_id_distance"),
        F.round(F.col("voltage_cume_dist") * 0.25, 2).alias("voltage_cume_dist_adj"),
    )
)

with_windows_2 = (
    labeled
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "rate",
        "energy_cost",
        "voltage_cume_dist",
        "kwh_share_of_site_id_distance",
        "voltage_cume_dist_adj",
        F.round(F.avg("voltage").over(w_site_id_by_voltage_prior_2), 2).alias("voltage_avg_2_prior"),
        F.round(F.col("rate") - F.avg("rate").over(w_read_at_month_zone), 2).alias("rate_vs_read_at_month_zone_avg"),
        F.round(F.col("voltage_cume_dist_adj") / F.nullif(F.sum("voltage_cume_dist_adj").over(w_read_at_month_zone), F.lit(0)), 4).alias("voltage_cume_dist_adj_share_of_read_at_month_zon"),
        F.dense_rank().over(w_site_id_by_voltage_ties).alias("voltage_dense_rank"),
        F.first("voltage", ignorenulls=True).over(w_site_id_by_voltage_running).alias("first_known_voltage"),
    )
)

result = (
    with_windows_2
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "energy_cost",
        "voltage_cume_dist_adj",
        "voltage_avg_2_prior",
        "rate_vs_read_at_month_zone_avg",
        "first_known_voltage",
    )
    .orderBy(F.col("rate_vs_read_at_month_zone_avg").asc_nulls_last())
)
