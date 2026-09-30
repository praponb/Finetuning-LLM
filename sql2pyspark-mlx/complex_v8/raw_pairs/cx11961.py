from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_kwh_bucket_ties = Window.orderBy(F.col("kwh_bucket").desc_nulls_first())
w_all_by_kwh_bucket_full = Window.orderBy(F.col("kwh_bucket").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_quality = Window.partitionBy("quality")
w_all_by_kwh_bucket_running = Window.orderBy(F.col("kwh_bucket").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_by_voltage_prior_1 = Window.partitionBy("quality").orderBy("voltage", "id").rowsBetween(-1, -1)
w_all_by_kwh_bucket_running_2 = Window.orderBy(F.col("kwh_bucket").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_by_voltage_ties = Window.partitionBy("quality").orderBy("voltage")
w_site_id = Window.partitionBy("site_id")
w_quality_by_voltage_running = Window.partitionBy("quality").orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_by_voltage = Window.partitionBy("quality").orderBy("voltage", "id")
w_all_by_kwh_bucket_ties_2 = Window.orderBy(F.col("kwh_bucket").desc())
w_voltage_dense_rank_by_second = Window.partitionBy("voltage_dense_rank").orderBy(F.col("second_kwh").desc(), "id").rowsBetween(-3, -1)
w_voltage_dense_rank = Window.partitionBy("voltage_dense_rank")
w_voltage_dense_rank_by_kwh_bu = Window.partitionBy("voltage_dense_rank").orderBy(F.col("kwh_bucket").desc(), "id").rowsBetween(-2, Window.currentRow)
w_voltage_dense_rank_by_kwh_bu_2 = Window.partitionBy("voltage_dense_rank").orderBy(F.col("kwh_bucket").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_voltage_dense_rank_by_second_2 = Window.partitionBy("voltage_dense_rank").orderBy(F.col("second_kwh").asc_nulls_last(), "id")
w_voltage_dense_rank_by_site_i = Window.partitionBy("voltage_dense_rank").orderBy("site_id_max_kwh_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_readings = (
    df_meter_readings
    .filter((F.col("kwh") > 20.4) & (F.col("site_id") >= 6))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.date_format(F.col("read_at"), "yyyy-MM-dd HH:00").alias("read_at_label"),
        (F.floor(F.col("kwh") / 25) * 25).alias("kwh_bucket"),
    )
)

metrics = (
    scoped_readings
    .select(
        "id",
        "voltage",
        "kwh_bucket",
        F.round(F.percent_rank().over(w_all_by_kwh_bucket_ties), 3).alias("kwh_bucket_percent_rank"),
        F.nth_value("kwh", 2).over(w_all_by_kwh_bucket_full).alias("second_kwh"),
        F.min("kwh").over(w_quality).alias("quality_min_kwh"),
        F.max("kwh_bucket").over(w_all_by_kwh_bucket_running).alias("max_kwh_bucket_so_far"),
        F.sum("voltage").over(w_quality).alias("quality_sum_voltage"),
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_quality), F.lit(0)), 4).alias("voltage_share_of_quality"),
        F.round(F.avg("kwh").over(w_quality_by_voltage_prior_1), 2).alias("kwh_avg_1_prior"),
        F.first("kwh_bucket", ignorenulls=True).over(w_all_by_kwh_bucket_running_2).alias("first_known_kwh_bucket"),
        F.dense_rank().over(w_quality_by_voltage_ties).alias("voltage_dense_rank"),
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_site_id), F.lit(0)), 4).alias("voltage_share_of_site_id"),
        F.last("kwh", ignorenulls=True).over(w_all_by_kwh_bucket_running_2).alias("last_known_kwh"),
        F.last("kwh", ignorenulls=True).over(w_quality_by_voltage_running).alias("last_known_kwh_2"),
        F.row_number().over(w_quality_by_voltage).alias("rn"),
        F.max("kwh_bucket").over(w_site_id).alias("site_id_max_kwh_bucket"),
        F.round(F.cume_dist().over(w_all_by_kwh_bucket_ties_2), 3).alias("kwh_bucket_cume_dist"),
    )
)

first_rows = (
    metrics
    .filter(F.col("rn") == 1)
    .drop("rn")
)

ranked = (
    first_rows
    .select(
        "id",
        "voltage",
        "kwh_bucket",
        "kwh_bucket_percent_rank",
        "second_kwh",
        "max_kwh_bucket_so_far",
        "first_known_kwh_bucket",
        "voltage_dense_rank",
        "voltage_share_of_site_id",
        "last_known_kwh",
        "last_known_kwh_2",
        "site_id_max_kwh_bucket",
        "kwh_bucket_cume_dist",
        F.round(F.avg("second_kwh").over(w_voltage_dense_rank_by_second), 2).alias("second_kwh_avg_3_prior"),
        F.sum("kwh_bucket_cume_dist").over(w_voltage_dense_rank).alias("voltage_dense_rank_sum_kwh_bucket_cume_dist"),
        F.sum("voltage_dense_rank").over(w_voltage_dense_rank_by_kwh_bu).alias("voltage_dense_rank_sum_3"),
        F.first("quality_sum_voltage").over(w_voltage_dense_rank_by_kwh_bu_2).alias("first_quality_sum_voltage"),
        F.lag("first_known_kwh_bucket", 1, 0).over(w_voltage_dense_rank_by_second_2).alias("prev_first_known_kwh_bucket"),
        F.round(F.avg("kwh_avg_1_prior").over(w_voltage_dense_rank), 2).alias("voltage_dense_rank_avg_kwh_avg_1_prior"),
        F.first("voltage", ignorenulls=True).over(w_voltage_dense_rank_by_site_i).alias("first_known_voltage"),
    )
)

result = (
    ranked
    .filter(F.col("kwh_bucket") >= 42)
    .select(
        "id",
        "kwh_bucket_percent_rank",
        "second_kwh",
        "first_known_kwh_bucket",
        "voltage_dense_rank",
        "voltage_share_of_site_id",
        "last_known_kwh",
        "last_known_kwh_2",
        "site_id_max_kwh_bucket",
        "second_kwh_avg_3_prior",
        "voltage_dense_rank_sum_kwh_bucket_cume_dist",
        "voltage_dense_rank_sum_3",
        "first_quality_sum_voltage",
        "prev_first_known_kwh_bucket",
        "voltage_dense_rank_avg_kwh_avg_1_prior",
        "first_known_voltage",
    )
    .orderBy(F.col("last_known_kwh").desc(), F.col("voltage_dense_rank").desc())
)
