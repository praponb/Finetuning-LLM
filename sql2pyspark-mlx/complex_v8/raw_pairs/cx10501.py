from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id")
w_site_id_quality_by_voltage_m = Window.partitionBy("site_id", "quality").orderBy(F.col("voltage_minus_kwh").desc())
w_quality = Window.partitionBy("quality")
w_all_by_voltage_minus_kwh = Window.orderBy(F.col("voltage_minus_kwh").asc_nulls_last(), "id")
w_quality_by_voltage_running = Window.partitionBy("quality").orderBy(F.col("voltage").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_by_voltage = Window.partitionBy("site_id").orderBy(F.col("voltage").asc_nulls_last(), "id")
w_all_by_voltage_minus_kwh_2 = Window.orderBy("voltage_minus_kwh", "id")
w_quality_site_id_by_voltage = Window.partitionBy("quality", "site_id").orderBy(F.col("voltage").desc(), "id")
w_quality_by_voltage = Window.partitionBy("quality").orderBy(F.col("voltage").asc_nulls_last(), "id")
w_site_id_quality = Window.partitionBy("site_id", "quality")
w_all_by_voltage_minus_kwh_tie = Window.orderBy("voltage_minus_kwh")
w_quality_by_voltage_minus_kwh = Window.partitionBy("quality").orderBy("voltage_minus_kwh", "id").rowsBetween(-3, -1)
w_all_by_rows_in_site_id_full = Window.orderBy(F.col("rows_in_site_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_rows_in_site_id_ties = Window.orderBy(F.col("rows_in_site_id").desc())
w_voltage_dense_rank_voltage_m = Window.partitionBy("voltage_dense_rank", "voltage_minus_kwh_dense_rank").orderBy("voltage_minus_kwh_share_of_quality", "id").rowsBetween(-1, Window.currentRow)
w_voltage_dense_rank_voltage_m_2 = Window.partitionBy("voltage_dense_rank", "voltage_minus_kwh_dense_rank").orderBy("voltage_minus_kwh_share_of_quality", "id")
w_all_by_rows_in_site_id = Window.orderBy(F.col("rows_in_site_id").desc(), "id")
w_voltage_dense_rank = Window.partitionBy("voltage_dense_rank")

valid_readings = (
    df_meter_readings
    .filter(F.col("voltage") >= 221.4)
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        (F.col("voltage") - F.col("kwh")).alias("voltage_minus_kwh"),
    )
)

windowed = (
    valid_readings
    .select(
        "*",
        F.round(F.avg("voltage").over(w_site_id), 2).alias("site_id_avg_voltage"),
        F.count(F.lit(1)).over(w_site_id).alias("rows_in_site_id"),
        F.rank().over(w_site_id_quality_by_voltage_m).alias("voltage_minus_kwh_rank"),
        F.round(F.col("voltage_minus_kwh") / F.nullif(F.sum("voltage_minus_kwh").over(w_quality), F.lit(0)), 4).alias("voltage_minus_kwh_share_of_quality"),
        F.min("voltage").over(w_site_id).alias("site_id_min_voltage"),
        F.dense_rank().over(w_all_by_voltage_minus_kwh).alias("voltage_minus_kwh_dense_rank"),
        F.last("voltage_minus_kwh", ignorenulls=True).over(w_quality_by_voltage_running).alias("last_known_voltage_minus_kwh"),
        F.lead("voltage_minus_kwh").over(w_site_id_by_voltage).alias("next_voltage_minus_kwh"),
        (F.col("voltage") - F.lag("voltage").over(w_all_by_voltage_minus_kwh_2)).alias("voltage_change"),
        F.lag("kwh").over(w_quality_site_id_by_voltage).alias("prev_kwh"),
        F.dense_rank().over(w_quality_by_voltage).alias("voltage_dense_rank"),
        F.dense_rank().over(w_all_by_voltage_minus_kwh_tie).alias("voltage_minus_kwh_dense_rank_2"),
        F.lead("voltage_minus_kwh", 2).over(w_quality_site_id_by_voltage).alias("next2_voltage_minus_kwh"),
    )
)

annotated = (
    windowed
    .filter(F.col("site_id").isNotNull())
    .select(
        "*",
        F.abs(F.col("voltage_change") - -24.5).alias("voltage_change_distance"),
        F.round(F.col("site_id_min_voltage") / F.nullif(F.col("prev_kwh"), F.lit(0)), 3).alias("site_id_min_voltage_per_prev_kwh"),
    )
)

windowed_2 = (
    annotated
    .select(
        "*",
        F.sum("voltage_change").over(w_quality_by_voltage_minus_kwh).alias("voltage_change_sum_3_prior"),
        F.last("prev_kwh").over(w_all_by_rows_in_site_id_full).alias("last_prev_kwh"),
        F.dense_rank().over(w_all_by_rows_in_site_id_ties).alias("rows_in_site_id_dense_rank"),
        F.sum("site_id_avg_voltage").over(w_voltage_dense_rank_voltage_m).alias("site_id_avg_voltage_sum_2"),
        (F.col("rows_in_site_id") - F.lag("rows_in_site_id").over(w_voltage_dense_rank_voltage_m_2)).alias("rows_in_site_id_change"),
        F.ntile(10).over(w_all_by_rows_in_site_id).alias("rows_in_site_id_decile"),
        F.count(F.lit(1)).over(w_voltage_dense_rank).alias("rows_in_voltage_dense_rank"),
    )
)

result = (
    windowed_2
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        "site_id_avg_voltage",
        "rows_in_site_id",
        "voltage_minus_kwh_rank",
        "voltage_minus_kwh_share_of_quality",
        "site_id_min_voltage",
        "voltage_minus_kwh_dense_rank",
        "last_known_voltage_minus_kwh",
        "next_voltage_minus_kwh",
        "voltage_change",
        "prev_kwh",
        "voltage_dense_rank",
        "voltage_minus_kwh_dense_rank_2",
        "next2_voltage_minus_kwh",
        "voltage_change_distance",
        "site_id_min_voltage_per_prev_kwh",
        "last_prev_kwh",
        "site_id_avg_voltage_sum_2",
        "rows_in_site_id_change",
        "rows_in_site_id_decile",
        "rows_in_voltage_dense_rank",
        F.abs(F.col("voltage_dense_rank") - 6).alias("voltage_dense_rank_distance"),
    )
    .orderBy("voltage_change_distance")
)
