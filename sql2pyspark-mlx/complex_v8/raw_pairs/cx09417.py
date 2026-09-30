from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id")
w_all_by_kwh = Window.orderBy(F.col("kwh").asc_nulls_last(), "id")
w_site_id_quality_by_kwh = Window.partitionBy("site_id", "quality").orderBy(F.col("kwh").asc_nulls_last(), "id")
w_all_by_kwh_moving_6 = Window.orderBy("kwh", "id").rowsBetween(-6, Window.currentRow)
w_all_by_kwh_running = Window.orderBy(F.col("kwh").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_quality_by_kwh_ties = Window.partitionBy("site_id", "quality").orderBy(F.col("kwh").asc_nulls_last())
w_site_id_quality_by_kwh_movin = Window.partitionBy("site_id", "quality").orderBy(F.col("kwh").asc_nulls_last(), "id").rowsBetween(-1, Window.currentRow)
w_all_by_kwh_full = Window.orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_quality_site_id_by_site_comm = Window.partitionBy("quality", "site_id").orderBy("site_commissioned_on", "id")
w_kwh_tercile_first_read_at_to = Window.partitionBy("kwh_tercile", F.to_date("first_read_at"))
w_kwh_quartile_3_by_kwh_dense = Window.partitionBy("kwh_quartile_3").orderBy(F.col("kwh_dense_rank").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_kwh_quartile_3_by_kwh_dense_2 = Window.partitionBy("kwh_quartile_3").orderBy(F.col("kwh_dense_rank").desc())
w_kwh_quartile_3_by_kwh_dense_3 = Window.partitionBy("kwh_quartile_3").orderBy(F.col("kwh_dense_rank").desc(), "id")

recent_readings = (
    df_meter_readings
    .filter(F.col("kwh") > 14.4)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

combined_details_site = (
    recent_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.*",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.installed_kw").alias("site_installed_kw"),
        "si.site_name",
    )
)

metrics = (
    combined_details_site
    .select(
        "*",
        F.sum("site_installed_kw").over(w_site_id).alias("site_id_sum_site_installed_kw"),
        F.dense_rank().over(w_all_by_kwh).alias("kwh_dense_rank"),
        F.rank().over(w_site_id_quality_by_kwh).alias("kwh_rank"),
        F.ntile(4).over(w_site_id_quality_by_kwh).alias("kwh_quartile"),
        F.round(F.avg("voltage").over(w_all_by_kwh_moving_6), 2).alias("voltage_avg_7"),
        F.count(F.lit(1)).over(w_all_by_kwh_running).alias("running_count"),
        F.ntile(3).over(w_all_by_kwh).alias("kwh_tercile"),
        F.dense_rank().over(w_site_id_quality_by_kwh_ties).alias("kwh_dense_rank_2"),
        F.round(F.avg("site_installed_kw").over(w_site_id_quality_by_kwh_movin), 2).alias("site_installed_kw_avg_2"),
        F.round(F.cume_dist().over(w_site_id_quality_by_kwh_ties), 3).alias("kwh_cume_dist"),
        F.ntile(4).over(w_all_by_kwh).alias("kwh_quartile_3"),
        F.first("read_at").over(w_all_by_kwh_full).alias("first_read_at"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_site_id), 2).alias("voltage_vs_site_id_avg"),
        F.count(F.lit(1)).over(w_site_id).alias("rows_in_site_id"),
    )
)

top_rows = metrics.filter(F.col("kwh_dense_rank_2") <= 3)

with_trends = (
    top_rows
    .select(
        "*",
        F.rank().over(w_quality_site_id_by_site_comm).alias("site_commissioned_on_rank"),
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_kwh_tercile_first_read_at_to), F.lit(0)), 4).alias("voltage_share_of_kwh_tercile_first_read_at_to_da"),
        F.min("kwh_rank").over(w_kwh_tercile_first_read_at_to).alias("kwh_tercile_first_read_at_to_date_min_kwh_rank"),
        F.nth_value("voltage_avg_7", 2).over(w_kwh_quartile_3_by_kwh_dense).alias("second_voltage_avg_7"),
        F.dense_rank().over(w_kwh_quartile_3_by_kwh_dense_2).alias("kwh_dense_rank_dense_rank"),
        F.count(F.lit(1)).over(w_kwh_tercile_first_read_at_to).alias("rows_in_kwh_tercile_first_read_at_to_date"),
        F.ntile(5).over(w_kwh_quartile_3_by_kwh_dense_3).alias("kwh_dense_rank_quintile"),
        F.ntile(3).over(w_kwh_quartile_3_by_kwh_dense_3).alias("kwh_dense_rank_tercile"),
    )
)

result = (
    with_trends
    .filter(F.col("kwh_rank") > 3)
    .select(
        "id",
        "read_at",
        "kwh",
        "quality",
        "site_commissioned_on",
        "site_installed_kw",
        "site_name",
        "site_id_sum_site_installed_kw",
        "kwh_rank",
        "kwh_quartile",
        "voltage_avg_7",
        "running_count",
        "kwh_dense_rank_2",
        "site_installed_kw_avg_2",
        "kwh_cume_dist",
        "first_read_at",
        "rows_in_site_id",
        "site_commissioned_on_rank",
        "kwh_tercile_first_read_at_to_date_min_kwh_rank",
        "second_voltage_avg_7",
        "kwh_dense_rank_dense_rank",
        "rows_in_kwh_tercile_first_read_at_to_date",
        "kwh_dense_rank_tercile",
        F.round((F.unix_timestamp(F.col("read_at")) - F.unix_timestamp(F.col("first_read_at"))) / 60, 1).alias("minutes_first_read_at_to_read_at"),
        F.round(F.col("kwh") / F.nullif(F.col("rows_in_site_id"), F.lit(0)), 3).alias("kwh_per_rows_in_site_id"),
    )
    .orderBy(F.col("quality").desc(), F.col("kwh_dense_rank_2").desc(), "id")
    .limit(5)
)
