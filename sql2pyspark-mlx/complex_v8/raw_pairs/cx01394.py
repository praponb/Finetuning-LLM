from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_month = Window.partitionBy(F.date_trunc("month", "read_at"))
w_all_by_rate_running = Window.orderBy(F.col("rate").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id = Window.partitionBy("site_id")

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

windowed = (
    joined_ranges
    .select(
        "*",
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_read_at_month), F.lit(0)), 4).alias("voltage_share_of_read_at_month"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_read_at_month), 2).alias("kwh_vs_read_at_month_avg"),
        F.last("read_at", ignorenulls=True).over(w_all_by_rate_running).alias("last_known_read_at"),
        F.count(F.lit(1)).over(w_read_at_month).alias("rows_in_read_at_month"),
    )
)

qualified = (
    windowed.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left_semi")
    .filter(F.col("rate") > 2.2)
)

with_trends = (
    qualified
    .select(
        "*",
        F.round(F.avg("kwh").over(w_site_id), 2).alias("site_id_avg_kwh"),
        F.size(F.collect_set("zone").over(w_site_id)).alias("distinct_zone_in_site_id"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "rate",
        "voltage_share_of_read_at_month",
        "kwh_vs_read_at_month_avg",
        "last_known_read_at",
        "rows_in_read_at_month",
        "site_id_avg_kwh",
        "distinct_zone_in_site_id",
        F.concat(F.col("zone"), F.lit("#"), F.col("id").cast("string")).alias("zone_ref"),
    )
    .orderBy("site_id_avg_kwh")
)
