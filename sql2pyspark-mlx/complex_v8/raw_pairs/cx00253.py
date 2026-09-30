from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_kwh_ties = Window.orderBy(F.col("kwh").desc_nulls_first())
w_all_by_voltage_ties = Window.orderBy("voltage")
w_quality_site_id = Window.partitionBy("quality", "site_id")
w_all_by_voltage = Window.orderBy("voltage", "id")
w_all_by_kwh = Window.orderBy(F.col("kwh").desc_nulls_first(), "id")
w_all_by_kwh_2 = Window.orderBy("kwh", "id")
w_all_by_total_kwh_decile = Window.orderBy(F.col("total_kwh_decile").asc_nulls_last(), "kwh_rank", "site_id")
w_site_id_by_total_quality_sit = Window.partitionBy("site_id").orderBy(F.col("total_quality_site_id_sum_voltage").desc(), "kwh_rank", "site_id").rowsBetween(-6, Window.currentRow)
w_all_by_total_kwh_decile_runn = Window.orderBy(F.col("total_kwh_decile").asc_nulls_last(), "kwh_rank", "site_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_readings = (
    df_meter_readings
    .filter((F.col("kwh") <= 106.0) & (F.col("voltage") >= 216.1))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
    )
)

with_windows = (
    scoped_readings
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        F.rank().over(w_all_by_kwh_ties).alias("kwh_rank"),
        F.rank().over(w_all_by_voltage_ties).alias("voltage_rank"),
        F.sum("voltage").over(w_quality_site_id).alias("quality_site_id_sum_voltage"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_quality_site_id), 2).alias("voltage_vs_quality_site_id_avg"),
        F.lag("kwh").over(w_all_by_voltage).alias("prev_kwh"),
        F.ntile(5).over(w_all_by_kwh).alias("kwh_quintile"),
        F.ntile(10).over(w_all_by_kwh_2).alias("kwh_decile"),
        F.ntile(4).over(w_all_by_kwh_2).alias("kwh_quartile"),
    )
)

first_rows = with_windows.filter((F.col("kwh_rank") == 1) & (F.col("kwh_quintile") <= 6))

totals = (
    first_rows
    .groupBy("kwh_rank", "site_id")
    .agg(
        F.round(F.sum("quality_site_id_sum_voltage"), 2).alias("total_quality_site_id_sum_voltage"),
        F.sum("kwh_decile").alias("total_kwh_decile"),
    )
)

windowed = (
    totals
    .select(
        "*",
        F.row_number().over(w_all_by_total_kwh_decile).alias("seq_no"),
        F.round(F.avg("total_kwh_decile").over(w_site_id_by_total_quality_sit), 2).alias("total_kwh_decile_avg_7"),
        F.last("total_kwh_decile", ignorenulls=True).over(w_all_by_total_kwh_decile_runn).alias("last_known_total_kwh_decile"),
    )
)

result = (
    windowed
    .select(
        "kwh_rank",
        "total_quality_site_id_sum_voltage",
        "total_kwh_decile",
        "total_kwh_decile_avg_7",
        F.coalesce(F.col("kwh_rank"), F.lit(0)).alias("kwh_rank_filled"),
    )
    .orderBy(
        "total_kwh_decile",
        F.col("total_quality_site_id_sum_voltage").desc_nulls_first(),
    )
)
