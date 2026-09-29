from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_month_quality = Window.partitionBy(F.month("read_at"), "quality")
w_site_id_by_read_at_prior_2 = Window.partitionBy("site_id").orderBy(F.col("read_at").desc(), "id").rowsBetween(-2, -1)
w_quality_site_id_by_voltage_t = Window.partitionBy("quality", "site_id").orderBy("voltage")
w_site_id_by_read_at = Window.partitionBy("site_id").orderBy(F.col("read_at").desc(), "id")
w_quality_site_id = Window.partitionBy("quality", "site_id")
w_site_id = Window.partitionBy("site_id")
w_quality_site_id_by_read_at = Window.partitionBy("quality", "site_id").orderBy("read_at", "id")
w_quality_by_read_at_moving_2 = Window.partitionBy("quality").orderBy(F.col("read_at").desc_nulls_first(), "id").rowsBetween(-2, Window.currentRow)
w_quality_by_read_at_ties = Window.partitionBy("quality").orderBy("read_at")
w_quality_site_id_by_read_at_t = Window.partitionBy("quality", "site_id").orderBy("read_at")
w_all_by_read_at_ties = Window.orderBy("read_at")
w_quality_by_read_at_cume_dist = Window.partitionBy("quality").orderBy("read_at_cume_dist", "id")
w_site_id_read_at_dense_rank = Window.partitionBy("site_id", "read_at_dense_rank")

recent = (
    df_meter_readings
    .filter(F.col("read_at") >= F.lit("2025-01-31 10:00:00").cast("timestamp"))
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
    )
)

windowed = (
    recent
    .select(
        "*",
        F.round(F.col("voltage") - F.avg("voltage").over(w_read_at_month_quality), 2).alias("voltage_vs_read_at_month_quality_avg"),
        F.round(F.avg("voltage").over(w_site_id_by_read_at_prior_2), 2).alias("voltage_avg_2_prior"),
        F.dense_rank().over(w_quality_site_id_by_voltage_t).alias("voltage_dense_rank"),
        F.dense_rank().over(w_site_id_by_read_at).alias("read_at_dense_rank"),
        F.lead("voltage").over(w_site_id_by_read_at).alias("next_voltage"),
        F.round(F.avg("voltage").over(w_site_id), 2).alias("site_id_avg_voltage"),
        F.ntile(3).over(w_quality_site_id_by_read_at).alias("read_at_tercile"),
        F.round(F.avg("voltage").over(w_quality_by_read_at_moving_2), 2).alias("voltage_avg_3"),
        F.round(F.percent_rank().over(w_quality_by_read_at_ties), 3).alias("read_at_percent_rank"),
        F.round(F.cume_dist().over(w_quality_site_id_by_read_at_t), 3).alias("read_at_cume_dist"),
        F.round(F.cume_dist().over(w_all_by_read_at_ties), 3).alias("read_at_cume_dist_2"),
    )
)

with_windows = (
    windowed
    .select(
        "*",
        F.row_number().over(w_quality_by_read_at_cume_dist).alias("rn"),
        F.size(F.collect_set("quality").over(w_site_id_read_at_dense_rank)).alias("distinct_quality_in_site_id_read_at_dense_rank"),
        (F.col("next_voltage") - F.lag("next_voltage").over(w_quality_by_read_at_cume_dist)).alias("next_voltage_change"),
        F.min("read_at_cume_dist").over(w_site_id_read_at_dense_rank).alias("site_id_read_at_dense_rank_min_read_at_cume_dist"),
    )
)

result = (
    with_windows
    .filter(F.col("voltage_dense_rank") > 4)
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        "voltage_avg_2_prior",
        "read_at_dense_rank",
        "next_voltage",
        "read_at_tercile",
        "voltage_avg_3",
        "read_at_percent_rank",
        "read_at_cume_dist",
        "distinct_quality_in_site_id_read_at_dense_rank",
        F.abs(F.col("read_at_percent_rank") - 0.5).alias("read_at_percent_rank_distance"),
        F.round(F.col("voltage") * 1.07, 2).alias("voltage_adj"),
    )
    .orderBy(F.col("next_voltage").desc(), "id")
    .limit(5)
)
