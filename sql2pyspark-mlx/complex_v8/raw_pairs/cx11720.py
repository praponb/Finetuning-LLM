from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_site_id = Window.partitionBy("quality", "site_id")
w_site_id_by_read_at_ties = Window.partitionBy("site_id").orderBy("read_at")
w_read_at_year_by_read_at_ties = Window.partitionBy(F.year("read_at")).orderBy("read_at")
w_site_id_by_read_at_running = Window.partitionBy("site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_read_at_year_by_read_at = Window.partitionBy(F.year("read_at")).orderBy("read_at", "id")
w_site_id_by_read_at_full = Window.partitionBy("site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_voltage = Window.orderBy(F.col("voltage").desc(), "id")
w_site_id_by_read_at = Window.partitionBy("site_id").orderBy("read_at", "id")
w_read_at_year_by_read_at_movi = Window.partitionBy(F.year("read_at")).orderBy("read_at", "id").rowsBetween(-2, Window.currentRow)
w_all_by_voltage_running = Window.orderBy(F.col("voltage").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_quality = Window.partitionBy("site_id", "quality")
w_read_at_tercile = Window.partitionBy("read_at_tercile")
w_quality_by_read_at_tercile_f = Window.partitionBy("quality").orderBy(F.col("read_at_tercile").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_quality_by_read_at_tercile = Window.partitionBy("quality").orderBy(F.col("read_at_tercile").desc_nulls_first(), "id")
w_read_at_rank_by_running_coun = Window.partitionBy("read_at_rank").orderBy("running_count", "id")
w_read_at_rank_by_running_coun_2 = Window.partitionBy("read_at_rank").orderBy("running_count", "id").rowsBetween(-2, -1)

recent_readings = (
    df_meter_readings
    .filter(F.col("site_id").isNotNull())
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        (F.floor(F.col("voltage") / 5) * 5).alias("voltage_bucket"),
        F.least(F.col("voltage"), F.col("kwh")).alias("least_voltage_kwh"),
    )
)

sequenced = (
    recent_readings
    .select(
        "*",
        F.round(F.col("voltage") - F.avg("voltage").over(w_quality_site_id), 2).alias("voltage_vs_quality_site_id_avg"),
        F.max("kwh").over(w_quality_site_id).alias("quality_site_id_max_kwh"),
        F.round(F.percent_rank().over(w_site_id_by_read_at_ties), 3).alias("read_at_percent_rank"),
        F.dense_rank().over(w_read_at_year_by_read_at_ties).alias("read_at_dense_rank"),
        F.count(F.lit(1)).over(w_site_id_by_read_at_running).alias("running_count"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_quality_site_id), F.lit(0)), 4).alias("kwh_share_of_quality_site_id"),
        F.ntile(3).over(w_read_at_year_by_read_at).alias("read_at_tercile"),
        F.nth_value("voltage", 2).over(w_site_id_by_read_at_full).alias("second_voltage"),
        F.first("least_voltage_kwh").over(w_site_id_by_read_at_full).alias("first_least_voltage_kwh"),
        F.row_number().over(w_all_by_voltage).alias("rn"),
        F.rank().over(w_site_id_by_read_at).alias("read_at_rank"),
        F.sum("voltage").over(w_read_at_year_by_read_at_movi).alias("voltage_sum_3"),
        F.last("least_voltage_kwh", ignorenulls=True).over(w_all_by_voltage_running).alias("last_known_least_voltage_kwh"),
        F.count(F.lit(1)).over(w_quality_site_id).alias("rows_in_quality_site_id"),
    )
)

with_trends = (
    sequenced
    .select(
        "*",
        F.count(F.lit(1)).over(w_site_id_quality).alias("rows_in_site_id_quality"),
        F.max("rows_in_quality_site_id").over(w_read_at_tercile).alias("read_at_tercile_max_rows_in_quality_site_id"),
        F.nth_value("quality_site_id_max_kwh", 2).over(w_quality_by_read_at_tercile_f).alias("second_quality_site_id_max_kwh"),
        F.lead("read_at", 2).over(w_quality_by_read_at_tercile).alias("next2_read_at"),
        (F.col("read_at_percent_rank") - F.lag("read_at_percent_rank").over(w_read_at_rank_by_running_coun)).alias("read_at_percent_rank_change"),
        F.round(F.avg("read_at_rank").over(w_read_at_rank_by_running_coun_2), 2).alias("read_at_rank_avg_2_prior"),
    )
)

result = (
    with_trends
    .filter(F.col("voltage_bucket").between(222, 238))
    .select(
        "id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        "voltage_bucket",
        "least_voltage_kwh",
        "quality_site_id_max_kwh",
        "read_at_dense_rank",
        "running_count",
        "read_at_tercile",
        "second_voltage",
        "first_least_voltage_kwh",
        "rn",
        "read_at_rank",
        "voltage_sum_3",
        "last_known_least_voltage_kwh",
        "rows_in_site_id_quality",
        "read_at_tercile_max_rows_in_quality_site_id",
        "next2_read_at",
        "read_at_percent_rank_change",
        "read_at_rank_avg_2_prior",
        (F.floor(F.col("kwh") / 25) * 25).alias("kwh_bucket"),
    )
    .orderBy("kwh", F.col("read_at_tercile").asc_nulls_last())
)
