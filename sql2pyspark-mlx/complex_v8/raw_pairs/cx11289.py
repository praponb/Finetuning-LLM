from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_month_site_id_by_med = Window.partitionBy(F.month("read_at"), "site_id").orderBy(F.col("median_voltage").desc())
w_read_at_month_site_id_by_med_2 = Window.partitionBy(F.month("read_at"), "site_id").orderBy(F.col("median_voltage").desc(), "id").rowsBetween(-1, Window.currentRow)
w_read_at_month_site_id_by_med_3 = Window.partitionBy(F.month("read_at"), "site_id").orderBy(F.col("median_voltage").desc(), "id")
w_quality_read_at_to_date_by_v = Window.partitionBy("quality", F.to_date("read_at")).orderBy("voltage_minus_median", "id")
w_quality = Window.partitionBy("quality")
w_all_by_read_at_full = Window.orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_rows_in_quality = Window.orderBy(F.col("rows_in_quality").desc(), "id")
w_all_by_read_at_running = Window.orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_readings = df_meter_readings.filter(F.col("kwh") <= 92.5)

site_id_stats = (
    recent_readings
    .groupBy("site_id")
    .agg(F.percentile_approx("voltage", 0.5).alias("median_voltage"))
)

above_average = (
    recent_readings.alias("s")
    .join(site_id_stats.alias("g"), F.col("g.site_id") == F.col("s.site_id"), "inner")
    .filter(F.col("s.voltage") >= F.col("g.median_voltage"))
    .select(
        "s.*",
        "g.median_voltage",
        F.round(F.col("s.voltage") - F.col("g.median_voltage"), 2).alias("voltage_minus_median"),
    )
)

windowed = (
    above_average
    .select(
        "*",
        F.dense_rank().over(w_read_at_month_site_id_by_med).alias("median_voltage_dense_rank"),
        F.round(F.avg("voltage_minus_median").over(w_read_at_month_site_id_by_med_2), 2).alias("voltage_minus_median_avg_2"),
        F.ntile(5).over(w_read_at_month_site_id_by_med_3).alias("median_voltage_quintile"),
        (F.col("kwh") - F.lag("kwh").over(w_quality_read_at_to_date_by_v)).alias("kwh_change"),
    )
)

sequenced = (
    windowed
    .select(
        "id",
        "read_at",
        "kwh",
        "median_voltage_dense_rank",
        "voltage_minus_median_avg_2",
        "kwh_change",
        F.round(F.col("kwh") - F.avg("kwh").over(w_quality), 2).alias("kwh_vs_quality_avg"),
        F.round(F.col("kwh_change") - F.avg("kwh_change").over(w_quality), 2).alias("kwh_change_vs_quality_avg"),
        F.count(F.lit(1)).over(w_quality).alias("rows_in_quality"),
    )
)

with_windows = (
    sequenced
    .select(
        "*",
        F.nth_value("rows_in_quality", 2).over(w_all_by_read_at_full).alias("second_rows_in_quality"),
        F.last("kwh").over(w_all_by_read_at_full).alias("last_kwh"),
        F.ntile(10).over(w_all_by_rows_in_quality).alias("rows_in_quality_decile"),
        F.last("median_voltage_dense_rank", ignorenulls=True).over(w_all_by_read_at_running).alias("last_known_median_voltage_dense_rank"),
    )
)

result = (
    with_windows
    .select(
        "id",
        F.col("second_rows_in_quality").alias("m_second_rows_in_quality"),
        F.col("median_voltage_dense_rank").alias("m_median_voltage_dense_rank"),
        F.col("rows_in_quality_decile").alias("rows_in_quality_decile_v"),
        F.col("rows_in_quality").alias("m_rows_in_quality"),
    )
    .unpivot(
        ids=["id"],
        values=["m_second_rows_in_quality", "m_median_voltage_dense_rank", "rows_in_quality_decile_v", "m_rows_in_quality"],
        variableColumnName="field",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
