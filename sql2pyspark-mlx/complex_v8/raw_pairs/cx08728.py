from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_start_date = Window.orderBy("start_date", "id")
w_all_by_end_date = Window.orderBy(F.col("end_date").desc(), "id")
w_all_by_start_date_running = Window.orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_id_by_start_date_runn = Window.partitionBy("holder_id").orderBy(F.col("start_date").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_holder_id_by_end_date = Window.partitionBy("status", "holder_id").orderBy(F.col("end_date").asc_nulls_last(), "id")
w_all_by_start_date_full = Window.orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_start_date_ties = Window.orderBy("start_date")
w_all_by_end_date_ties = Window.orderBy(F.col("end_date").desc())
w_second_start_date_month_end = Window.partitionBy(F.month("second_start_date"), F.date_trunc("month", "end_date")).orderBy("last_known_end_date", "id").rowsBetween(-3, -1)
w_prev2_start_date_month = Window.partitionBy(F.month("prev2_start_date"))
w_end_date_dense_rank_status_b = Window.partitionBy("end_date_dense_rank", "status").orderBy(F.col("second_start_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared_policys = (
    df_policies
    .filter((F.col("start_date") >= F.lit("2025-01-05").cast("date")) & (F.col("end_date") >= F.lit("2026-01-24").cast("date")))
    .select(
        "id",
        "holder_id",
        "start_date",
        "end_date",
        "status",
        F.concat(F.col("product"), F.lit("#"), F.col("id").cast("string")).alias("product_ref"),
        F.concat(F.col("status"), F.lit("#"), F.col("holder_id").cast("string")).alias("status_ref"),
    )
)

sequenced = (
    prepared_policys
    .select(
        "id",
        "start_date",
        "end_date",
        "status",
        "product_ref",
        "status_ref",
        F.round(F.cume_dist().over(w_all_by_start_date), 3).alias("start_date_cume_dist"),
        F.round(F.cume_dist().over(w_all_by_end_date), 3).alias("end_date_cume_dist"),
        F.last("end_date", ignorenulls=True).over(w_all_by_start_date_running).alias("last_known_end_date"),
        F.first("end_date", ignorenulls=True).over(w_holder_id_by_start_date_runn).alias("first_known_end_date"),
        F.lag("start_date", 2).over(w_status_holder_id_by_end_date).alias("prev2_start_date"),
        F.nth_value("start_date", 2).over(w_all_by_start_date_full).alias("second_start_date"),
        F.last("end_date", ignorenulls=True).over(w_holder_id_by_start_date_runn).alias("last_known_end_date_2"),
        F.dense_rank().over(w_all_by_start_date_ties).alias("start_date_dense_rank"),
        F.dense_rank().over(w_status_holder_id_by_end_date).alias("end_date_dense_rank"),
        F.row_number().over(w_all_by_end_date).alias("rn"),
        F.rank().over(w_all_by_end_date_ties).alias("end_date_rank"),
    )
)

derived = sequenced.withColumn("status_prefix", F.substring(F.trim(F.col("status")), 1, 2))

with_trends = (
    derived
    .select(
        "*",
        F.max("end_date_cume_dist").over(w_second_start_date_month_end).alias("end_date_cume_dist_max_3_prior"),
        F.size(F.collect_set("status").over(w_prev2_start_date_month)).alias("distinct_status_in_prev2_start_date_month"),
        F.count(F.lit(1)).over(w_prev2_start_date_month).alias("rows_in_prev2_start_date_month"),
        F.sum("end_date_rank").over(w_end_date_dense_rank_status_b).alias("running_end_date_rank"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "start_date",
        "end_date",
        "product_ref",
        "status_ref",
        "start_date_cume_dist",
        "end_date_cume_dist",
        "last_known_end_date",
        "prev2_start_date",
        "second_start_date",
        "last_known_end_date_2",
        "start_date_dense_rank",
        "rn",
        "status_prefix",
        "end_date_cume_dist_max_3_prior",
        "distinct_status_in_prev2_start_date_month",
        "rows_in_prev2_start_date_month",
    )
    .orderBy("end_date_cume_dist")
)
