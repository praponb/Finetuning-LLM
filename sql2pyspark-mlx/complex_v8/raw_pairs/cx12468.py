from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_energy_cost = Window.orderBy("energy_cost", "id")
w_all_by_energy_cost_full = Window.orderBy("energy_cost", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_read_at_month = Window.partitionBy(F.month("read_at"))
w_all_by_energy_cost_running = Window.orderBy("energy_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

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

qualified = (
    in_window.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.quality") != "EST"), "left_semi")
)

with_trends = (
    qualified
    .select(
        "*",
        F.round(F.cume_dist().over(w_all_by_energy_cost), 3).alias("energy_cost_cume_dist"),
        F.nth_value("rate", 2).over(w_all_by_energy_cost_full).alias("second_rate"),
        F.count(F.lit(1)).over(w_read_at_month).alias("rows_in_read_at_month"),
        F.row_number().over(w_all_by_energy_cost).alias("rn"),
        F.last("rate", ignorenulls=True).over(w_all_by_energy_cost_running).alias("last_known_rate"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "zone",
        "rate",
        "energy_cost",
        "energy_cost_cume_dist",
        "last_known_rate",
        F.abs(F.col("energy_cost_cume_dist") - 0.5).alias("energy_cost_cume_dist_distance"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("read_at")).alias("days_since_read_at"),
    )
    .orderBy(F.col("read_at").desc_nulls_first(), F.col("energy_cost").desc())
)
