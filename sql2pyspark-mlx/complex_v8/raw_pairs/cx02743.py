from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id")
w_site_id_by_kwh_full = Window.partitionBy("site_id").orderBy(F.col("kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id_by_read_at_ties = Window.partitionBy("site_id").orderBy(F.col("read_at").desc())
w_site_id_by_read_at_running = Window.partitionBy("site_id").orderBy(F.col("read_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_read_at_cume_dist = Window.orderBy(F.col("read_at_cume_dist").desc(), "id")
w_all_by_read_at_cume_dist_tie = Window.orderBy(F.col("read_at_cume_dist").desc())
w_all_by_read_at_cume_dist_pri = Window.orderBy(F.col("read_at_cume_dist").desc(), "id").rowsBetween(-2, -1)

filtered_readings = (
    df_meter_readings
    .filter(F.col("voltage") >= 213.7)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.coalesce(F.col("kwh"), F.lit(0.0)).alias("kwh_filled"),
        F.initcap(F.trim(F.col("quality"))).alias("quality_clean"),
    )
)

ranked = (
    filtered_readings
    .select(
        "id",
        "read_at",
        "kwh",
        "voltage",
        "kwh_filled",
        F.round(F.col("kwh_filled") / F.nullif(F.sum("kwh_filled").over(w_site_id), F.lit(0)), 4).alias("kwh_filled_share_of_site_id"),
        F.nth_value("voltage", 2).over(w_site_id_by_kwh_full).alias("second_voltage"),
        F.round(F.cume_dist().over(w_site_id_by_read_at_ties), 3).alias("read_at_cume_dist"),
        F.sum("kwh_filled").over(w_site_id_by_read_at_running).alias("running_kwh_filled"),
        F.count(F.lit(1)).over(w_site_id).alias("rows_in_site_id"),
    )
)

with_windows = (
    ranked
    .select(
        "*",
        (F.col("second_voltage") - F.lag("second_voltage").over(w_all_by_read_at_cume_dist)).alias("second_voltage_change"),
        F.dense_rank().over(w_all_by_read_at_cume_dist_tie).alias("read_at_cume_dist_dense_rank"),
        F.lag("running_kwh_filled", 3, 0).over(w_all_by_read_at_cume_dist).alias("prev3_running_kwh_filled"),
        F.row_number().over(w_all_by_read_at_cume_dist).alias("rn"),
        F.sum("running_kwh_filled").over(w_all_by_read_at_cume_dist_pri).alias("running_kwh_filled_sum_2_prior"),
        F.rank().over(w_all_by_read_at_cume_dist).alias("read_at_cume_dist_rank"),
    )
)

annotated = (
    with_windows
    .filter(F.col("read_at_cume_dist") <= 0.7)
    .select(
        "*",
        F.date_format(F.col("read_at"), "yyyy-MM-dd HH:00").alias("read_at_label"),
        (F.floor(F.col("running_kwh_filled") / 250) * 250).alias("running_kwh_filled_bucket"),
    )
)

result = (
    annotated
    .groupingSets(
        [["rn", "read_at_cume_dist_dense_rank"], ["rn"], []],
        "rn", "read_at_cume_dist_dense_rank",
    )
    .agg(F.grouping_id().alias("gid"), F.max("read_at").alias("last_read_at"))
    .orderBy(
        F.col("rn").asc_nulls_last(),
        F.col("read_at_cume_dist_dense_rank").asc_nulls_last(),
    )
)
