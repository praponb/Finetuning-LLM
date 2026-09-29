from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_read_at_moving_2 = Window.orderBy(F.col("read_at").desc_nulls_first(), "id").rowsBetween(-2, Window.currentRow)
w_all_by_read_at_ties = Window.orderBy(F.col("read_at").desc_nulls_first())
w_all_by_read_at_running = Window.orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_kwh = Window.orderBy("kwh", "id")
w_site_id = Window.partitionBy("site_id")
w_all_by_read_at = Window.orderBy(F.col("read_at").desc_nulls_first(), "id")

base = (
    df_meter_readings
    .filter(F.col("kwh") > 45.0)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.lower(F.trim(F.col("quality"))).alias("quality_clean"),
    )
)

windowed = (
    base
    .select(
        "id",
        "site_id",
        "kwh",
        F.round(F.avg("kwh").over(w_all_by_read_at_moving_2), 2).alias("kwh_avg_3"),
        F.round(F.percent_rank().over(w_all_by_read_at_ties), 3).alias("read_at_percent_rank"),
        F.rank().over(w_all_by_read_at_ties).alias("read_at_rank"),
        F.last("kwh", ignorenulls=True).over(w_all_by_read_at_running).alias("last_known_kwh"),
        F.round(F.percent_rank().over(w_all_by_kwh), 3).alias("kwh_percent_rank"),
        F.dense_rank().over(w_all_by_kwh).alias("kwh_dense_rank"),
        F.size(F.collect_set("quality").over(w_site_id)).alias("distinct_quality_in_site_id"),
        F.round(F.cume_dist().over(w_all_by_read_at_ties), 3).alias("read_at_cume_dist"),
        F.round(F.avg("kwh").over(w_site_id), 2).alias("site_id_avg_kwh"),
        F.row_number().over(w_all_by_kwh).alias("kwh_seq"),
        F.row_number().over(w_all_by_read_at).alias("rn"),
    )
)

result = (
    windowed
    .select(
        "id",
        "site_id",
        "kwh",
        "kwh_avg_3",
        "read_at_percent_rank",
        "last_known_kwh",
        "kwh_percent_rank",
        "kwh_dense_rank",
        "distinct_quality_in_site_id",
        "read_at_cume_dist",
        "site_id_avg_kwh",
        "kwh_seq",
        "rn",
        F.abs(F.col("last_known_kwh") - 58.5).alias("last_known_kwh_distance"),
        (F.floor(F.col("site_id_avg_kwh") / 25) * 25).alias("site_id_avg_kwh_bucket"),
    )
    .orderBy("kwh_seq", "id")
    .limit(5)
)
