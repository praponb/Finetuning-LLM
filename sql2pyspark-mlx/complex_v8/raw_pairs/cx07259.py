from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_by_voltage_minus_kwh = Window.partitionBy("site_id").orderBy(F.col("voltage_minus_kwh").asc_nulls_last(), "id")
w_read_at_month_site_id_by_kwh = Window.partitionBy(F.month("read_at"), "site_id").orderBy(F.col("kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_read_at_to_date_site_id_by_r = Window.partitionBy(F.to_date("read_at"), "site_id").orderBy(F.col("read_at").desc(), "id")
w_read_at_month = Window.partitionBy(F.date_trunc("month", "read_at"))

base_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        (F.col("voltage") - F.col("kwh")).alias("voltage_minus_kwh"),
        F.round(F.col("voltage") * 0.9, 2).alias("voltage_adj"),
    )
)

without_matches = (
    base_readings.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left_semi")
)

tagged = (
    without_matches
    .filter(F.col("voltage_adj") >= 273.0)
    .select(
        "*",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("read_at")).alias("days_since_read_at"),
        (F.floor(F.col("voltage_adj") / 25) * 25).alias("voltage_adj_bucket"),
        F.abs(F.col("kwh") - 51.0).alias("kwh_distance"),
    )
)

with_windows = (
    tagged
    .select(
        "*",
        F.row_number().over(w_site_id_by_voltage_minus_kwh).alias("rn"),
        F.first("kwh", ignorenulls=True).over(w_read_at_month_site_id_by_kwh).alias("first_known_kwh"),
        F.ntile(3).over(w_read_at_to_date_site_id_by_r).alias("read_at_tercile"),
        F.max("days_since_read_at").over(w_read_at_month).alias("read_at_month_max_days_since_read_at"),
        F.max("voltage_adj").over(w_read_at_month).alias("read_at_month_max_voltage_adj"),
    )
)

result = (
    with_windows
    .cube("read_at_tercile", "rn")
    .agg(
        F.min("first_known_kwh").alias("min_first_known_kwh"),
        F.percentile_approx("kwh_distance", 0.5).alias("median_kwh_distance"),
        F.max("read_at").alias("last_read_at"),
    )
    .orderBy(F.col("read_at_tercile").asc_nulls_last(), F.col("rn").asc_nulls_last())
)
