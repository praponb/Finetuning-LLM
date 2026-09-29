from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_voltage_filled_ties = Window.orderBy("voltage_filled")
w_site_id_quality_by_voltage = Window.partitionBy("site_id", "quality").orderBy("voltage", "id")
w_read_at_month_site_id = Window.partitionBy(F.month("read_at"), "site_id")
w_all_by_voltage_filled_moving = Window.orderBy("voltage_filled", "id").rowsBetween(-2, Window.currentRow)
w_all_by_voltage_filled = Window.orderBy("voltage_filled", "id")
w_site_id_quality_by_voltage_m = Window.partitionBy("site_id", "quality").orderBy("voltage", "id").rowsBetween(-3, Window.currentRow)
w_site_id = Window.partitionBy("site_id")
w_site_id_quality_by_voltage_r = Window.partitionBy("site_id", "quality").orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_voltage_filled_moving_2 = Window.orderBy("voltage_filled", "id").rowsBetween(-6, Window.currentRow)
w_all_by_kwh = Window.orderBy("kwh", "id")
w_all_by_voltage_filled_2 = Window.orderBy(F.col("voltage_filled").desc(), "id")
w_quality_site_id_by_voltage_v = Window.partitionBy("quality", "site_id").orderBy("voltage_vs_site_id_avg")
w_quality_site_id_by_voltage_v_2 = Window.partitionBy("quality", "site_id").orderBy("voltage_vs_site_id_avg", "id")
w_all_by_kwh_filled_avg_4 = Window.orderBy(F.col("kwh_filled_avg_4").asc_nulls_last(), "id")
w_quality = Window.partitionBy("quality")
w_site_id_by_voltage_rank_full = Window.partitionBy("site_id").orderBy("voltage_rank", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned_readings = (
    df_meter_readings
    .filter((F.col("read_at") < F.lit("2025-03-08 12:00:00").cast("timestamp")) & (F.col("kwh") <= 109.0))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.coalesce(F.col("kwh"), F.lit(0.0)).alias("kwh_filled"),
        F.coalesce(F.col("voltage"), F.lit(0.0)).alias("voltage_filled"),
    )
)

with_windows = (
    cleaned_readings
    .select(
        "*",
        F.dense_rank().over(w_all_by_voltage_filled_ties).alias("voltage_filled_dense_rank"),
        F.row_number().over(w_site_id_quality_by_voltage).alias("rn"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_read_at_month_site_id), F.lit(0)), 4).alias("kwh_share_of_read_at_month_site_id"),
        F.round(F.avg("kwh_filled").over(w_all_by_voltage_filled_moving), 2).alias("kwh_filled_avg_3"),
        (F.col("voltage") - F.lag("voltage").over(w_all_by_voltage_filled)).alias("voltage_change"),
        F.round(F.avg("kwh_filled").over(w_site_id_quality_by_voltage_m), 2).alias("kwh_filled_avg_4"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_site_id), F.lit(0)), 4).alias("kwh_share_of_site_id"),
        F.sum("voltage").over(w_site_id_quality_by_voltage_r).alias("running_voltage"),
        F.round(F.avg("voltage").over(w_all_by_voltage_filled_moving_2), 2).alias("voltage_avg_7"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_site_id), 2).alias("voltage_vs_site_id_avg"),
        F.round(F.percent_rank().over(w_all_by_kwh), 3).alias("kwh_percent_rank"),
        F.rank().over(w_site_id_quality_by_voltage).alias("voltage_rank"),
        F.lead("kwh", 2).over(w_all_by_voltage_filled_2).alias("next2_kwh"),
    )
)

windowed = (
    with_windows
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        "voltage_filled",
        "rn",
        "voltage_change",
        "voltage_vs_site_id_avg",
        "voltage_rank",
        "next2_kwh",
        F.dense_rank().over(w_quality_site_id_by_voltage_v).alias("voltage_vs_site_id_avg_dense_rank"),
        (F.col("next2_kwh") - F.lag("next2_kwh").over(w_quality_site_id_by_voltage_v_2)).alias("next2_kwh_change"),
        F.round(F.cume_dist().over(w_all_by_kwh_filled_avg_4), 3).alias("kwh_filled_avg_4_cume_dist"),
        F.size(F.collect_set("site_id").over(w_quality)).alias("distinct_site_id_in_quality"),
        F.first("kwh_filled").over(w_site_id_by_voltage_rank_full).alias("first_kwh_filled"),
        F.lag("voltage_filled", 3).over(w_all_by_kwh_filled_avg_4).alias("prev3_voltage_filled"),
    )
)

result = (
    windowed
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage_filled",
        "rn",
        "voltage_change",
        "voltage_vs_site_id_avg",
        "voltage_rank",
        "next2_kwh",
        "voltage_vs_site_id_avg_dense_rank",
        "next2_kwh_change",
        "kwh_filled_avg_4_cume_dist",
        "distinct_site_id_in_quality",
        "first_kwh_filled",
        "prev3_voltage_filled",
        F.upper(F.trim(F.col("quality"))).alias("quality_clean"),
    )
    .orderBy(F.col("voltage_rank").desc())
)
