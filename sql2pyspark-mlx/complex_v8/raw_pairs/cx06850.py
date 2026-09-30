from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_month = Window.partitionBy(F.month("read_at"))
w_all_by_days_since_read_at = Window.orderBy(F.col("days_since_read_at").desc_nulls_first(), "id")
w_all_by_voltage_adj_ties = Window.orderBy(F.col("voltage_adj").desc())
w_site_id = Window.partitionBy("site_id")
w_all_by_read_at_moving_2 = Window.orderBy(F.col("read_at").desc(), "id").rowsBetween(-2, Window.currentRow)
w_all_by_read_at_full = Window.orderBy(F.col("read_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id_by_second_voltage_ad = Window.partitionBy("site_id").orderBy("second_voltage_adj", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_second_voltage_adj = Window.orderBy(F.col("second_voltage_adj").desc(), "id")

prepared = (
    df_meter_readings
    .filter(F.col("voltage").between(215.7, 240.9))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.round(F.col("voltage") * 1.5, 2).alias("voltage_adj"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("read_at")).alias("days_since_read_at"),
        F.substring(F.trim(F.col("quality")), 1, 4).alias("quality_prefix"),
    )
)

with_trends = (
    prepared
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage_adj",
        "days_since_read_at",
        F.round(F.col("kwh") - F.avg("kwh").over(w_read_at_month), 2).alias("kwh_vs_read_at_month_avg"),
        F.lag("read_at").over(w_all_by_days_since_read_at).alias("prev_read_at"),
    )
)

qualified = (
    with_trends.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left_semi")
)

with_windows = (
    qualified
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage_adj",
        "days_since_read_at",
        "kwh_vs_read_at_month_avg",
        "prev_read_at",
        F.round(F.percent_rank().over(w_all_by_voltage_adj_ties), 3).alias("voltage_adj_percent_rank"),
        F.count(F.lit(1)).over(w_site_id).alias("rows_in_site_id"),
        F.max("kwh").over(w_all_by_read_at_moving_2).alias("kwh_max_3"),
        F.nth_value("voltage_adj", 2).over(w_all_by_read_at_full).alias("second_voltage_adj"),
    )
)

with_activity = (
    with_windows.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.month(F.col("m.read_at")).isin(2, 3)) & (F.col("m.voltage") < 233.6), "left_anti")
)

windowed = (
    with_activity
    .select(
        "*",
        F.last("kwh_vs_read_at_month_avg", ignorenulls=True).over(w_site_id_by_second_voltage_ad).alias("last_known_kwh_vs_read_at_month_avg"),
        F.ntile(10).over(w_all_by_second_voltage_adj).alias("second_voltage_adj_decile"),
        F.ntile(4).over(w_all_by_second_voltage_adj).alias("second_voltage_adj_quartile"),
    )
)

result = (
    windowed
    .filter(F.col("kwh_max_3") < 234.0)
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage_adj",
        "days_since_read_at",
        "kwh_vs_read_at_month_avg",
        "prev_read_at",
        "voltage_adj_percent_rank",
        "rows_in_site_id",
        "kwh_max_3",
        "second_voltage_adj",
        "last_known_kwh_vs_read_at_month_avg",
        "second_voltage_adj_decile",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("prev_read_at")).alias("days_since_prev_read_at"),
        F.round(F.col("rows_in_site_id") / F.nullif(F.col("second_voltage_adj"), F.lit(0)), 3).alias("rows_in_site_id_per_second_voltage_adj"),
    )
    .orderBy(F.col("read_at").asc_nulls_last())
)
