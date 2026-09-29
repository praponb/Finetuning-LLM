from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_by_voltage = Window.partitionBy("site_id").orderBy("voltage", "id")
w_all_by_voltage_moving_3 = Window.orderBy(F.col("voltage").desc(), "id").rowsBetween(-3, Window.currentRow)
w_quality_site_id = Window.partitionBy("quality", "site_id")
w_site_id_by_voltage_moving_6 = Window.partitionBy("site_id").orderBy("voltage", "id").rowsBetween(-6, Window.currentRow)
w_quality_by_voltage = Window.partitionBy("quality").orderBy("voltage", "id")
w_site_id_by_voltage_full = Window.partitionBy("site_id").orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_voltage_ties = Window.orderBy(F.col("voltage").desc())
w_site_id_by_voltage_ties = Window.partitionBy("site_id").orderBy("voltage")
w_all_by_voltage = Window.orderBy(F.col("voltage").desc(), "id")
w_quality_by_voltage_running = Window.partitionBy("quality").orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_rn = Window.partitionBy("rn")
w_all_by_quality_site_id_min_k = Window.orderBy("quality_site_id_min_kwh", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_quality_site_id_min_k_2 = Window.orderBy("quality_site_id_min_kwh")

base_readings = (
    df_meter_readings
    .filter(F.month(F.col("read_at")).isin(1, 3))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
    )
)

metrics = (
    base_readings
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.lead("kwh", 1, 0).over(w_site_id_by_voltage).alias("next_kwh"),
        F.sum("voltage").over(w_all_by_voltage_moving_3).alias("voltage_sum_4"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_quality_site_id), 2).alias("kwh_vs_quality_site_id_avg"),
        F.round(F.avg("voltage").over(w_site_id_by_voltage_moving_6), 2).alias("voltage_avg_7"),
        F.round(F.cume_dist().over(w_quality_by_voltage), 3).alias("voltage_cume_dist"),
        F.first("kwh").over(w_site_id_by_voltage_full).alias("first_kwh"),
        F.row_number().over(w_site_id_by_voltage).alias("rn"),
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_quality_site_id), F.lit(0)), 4).alias("voltage_share_of_quality_site_id"),
        F.rank().over(w_all_by_voltage_ties).alias("voltage_rank"),
        F.round(F.percent_rank().over(w_site_id_by_voltage_ties), 3).alias("voltage_percent_rank"),
        F.round(F.percent_rank().over(w_all_by_voltage), 3).alias("voltage_percent_rank_2"),
        F.ntile(10).over(w_quality_by_voltage).alias("voltage_decile"),
        F.min("kwh").over(w_quality_site_id).alias("quality_site_id_min_kwh"),
        (F.col("voltage") - F.lag("voltage").over(w_quality_by_voltage)).alias("voltage_change"),
        F.first("voltage", ignorenulls=True).over(w_quality_by_voltage_running).alias("first_known_voltage"),
    )
)

windowed = (
    metrics
    .select(
        "*",
        F.round(F.avg("voltage_cume_dist").over(w_rn), 2).alias("rn_avg_voltage_cume_dist"),
        F.first("next_kwh").over(w_all_by_quality_site_id_min_k).alias("first_next_kwh"),
        F.rank().over(w_all_by_quality_site_id_min_k_2).alias("quality_site_id_min_kwh_rank"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_rn), 2).alias("kwh_vs_rn_avg"),
    )
)

result = (
    windowed
    .select(
        "id",
        "site_id",
        "kwh",
        "next_kwh",
        "voltage_sum_4",
        "voltage_avg_7",
        "voltage_cume_dist",
        "rn",
        "voltage_share_of_quality_site_id",
        "voltage_percent_rank_2",
        "voltage_decile",
        "quality_site_id_min_kwh",
        "voltage_change",
        "first_known_voltage",
        "rn_avg_voltage_cume_dist",
        F.round(F.col("quality_site_id_min_kwh") / F.nullif(F.col("rn"), F.lit(0)), 3).alias("quality_site_id_min_kwh_per_rn"),
        F.concat(F.col("quality"), F.lit("#"), F.col("site_id").cast("string")).alias("quality_ref"),
    )
    .orderBy(F.col("site_id").asc())
)
