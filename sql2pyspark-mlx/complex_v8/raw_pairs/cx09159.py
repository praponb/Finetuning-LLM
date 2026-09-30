from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id")
w_site_id_quality_by_voltage = Window.partitionBy("site_id", "quality").orderBy(F.col("voltage").desc(), "id")
w_quality_by_voltage = Window.partitionBy("quality").orderBy(F.col("voltage").asc_nulls_last(), "id")
w_quality_site_id = Window.partitionBy("quality", "site_id")
w_quality_by_kwh_ties = Window.partitionBy("quality").orderBy(F.col("kwh").desc())
w_site_id_quality_by_voltage_f = Window.partitionBy("site_id", "quality").orderBy(F.col("voltage").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_quality_by_n_quality_est_tie = Window.partitionBy("quality").orderBy(F.col("n_quality_est").asc_nulls_last())
w_quality_n_quality_est_by_max = Window.partitionBy("quality", "n_quality_est").orderBy(F.col("max_voltage_vs_site_id_avg").desc(), "voltage_decile_2", "quality", "read_at_ym").rowsBetween(-6, Window.currentRow)

scoped_readings = (
    df_meter_readings
    .filter(F.col("kwh") < 89.8)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

with_windows = (
    scoped_readings
    .select(
        "*",
        F.count(F.lit(1)).over(w_site_id).alias("rows_in_site_id"),
        F.round(F.cume_dist().over(w_site_id_quality_by_voltage), 3).alias("voltage_cume_dist"),
        F.max("kwh").over(w_site_id).alias("site_id_max_kwh"),
        F.ntile(10).over(w_site_id_quality_by_voltage).alias("voltage_decile"),
        F.ntile(10).over(w_quality_by_voltage).alias("voltage_decile_2"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_site_id), 2).alias("kwh_vs_site_id_avg"),
        F.sum("voltage").over(w_site_id).alias("site_id_sum_voltage"),
        F.count(F.lit(1)).over(w_quality_site_id).alias("rows_in_quality_site_id"),
        F.rank().over(w_quality_by_kwh_ties).alias("kwh_rank"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_site_id), 2).alias("voltage_vs_site_id_avg"),
        F.first("kwh").over(w_site_id_quality_by_voltage_f).alias("first_kwh"),
    )
)

stats = (
    with_windows
    .filter(F.col("site_id_sum_voltage") <= 1442.0)
    .groupBy(
        "voltage_decile_2",
        "quality",
        F.date_format("read_at", "yyyy-MM").alias("read_at_ym"),
    )
    .agg(
        F.count(F.when(F.col("quality") == "EST", 1)).alias("n_quality_est"),
        F.max("voltage_vs_site_id_avg").alias("max_voltage_vs_site_id_avg"),
    )
)

windowed = (
    stats
    .select(
        "*",
        F.dense_rank().over(w_quality_by_n_quality_est_tie).alias("n_quality_est_dense_rank"),
        F.round(F.avg("n_quality_est").over(w_quality_n_quality_est_by_max), 2).alias("n_quality_est_avg_7"),
    )
)

result = (
    windowed
    .select(
        "voltage_decile_2",
        "quality",
        "read_at_ym",
        "n_quality_est",
        "max_voltage_vs_site_id_avg",
        "n_quality_est_dense_rank",
        F.round(F.col("n_quality_est_avg_7") * 0.9, 2).alias("n_quality_est_avg_7_adj"),
    )
)
