from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_by_premium_ties = Window.partitionBy("status").orderBy(F.col("premium").desc())
w_status_group_status_by_start = Window.partitionBy("status_group", "status").orderBy(F.col("start_date").desc())
w_status_group = Window.partitionBy("status_group")
w_status_by_premium = Window.partitionBy("status").orderBy(F.col("premium").desc(), "id")
w_all_by_premium = Window.orderBy("premium", "id")
w_all_by_premium_full = Window.orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_premium_ties = Window.orderBy("premium")
w_product_by_start_date_runnin = Window.partitionBy("product").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_group_holder_id = Window.partitionBy("status_group", "holder_id")
w_all_by_premium_running = Window.orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_id_by_premium_change = Window.partitionBy("holder_id").orderBy("premium_change", "id")
w_all_by_rows_in_status_group = Window.orderBy("rows_in_status_group", "id")
w_holder_id_by_premium_change_2 = Window.partitionBy("holder_id").orderBy("premium_change", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_next3_start_date_year = Window.partitionBy(F.year("next3_start_date"))
w_all_by_rows_in_status_group_2 = Window.orderBy("rows_in_status_group")
w_premium_tercile_status_by_pr = Window.partitionBy("premium_tercile", "status").orderBy("premium_tercile", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_policys = (
    df_policies
    .filter((F.col("product").isin("AUTO", "HEALTH")) & (F.col("premium") <= 3532.7))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
        F.when(F.col("status").isin("LAPSED", "CANCELLED"), "group_a").when(F.col("status") == "EXPIRED", "group_b").otherwise("other").alias("status_group"),
    )
)

with_trends = (
    filtered_policys
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "status",
        "status_group",
        F.dense_rank().over(w_status_by_premium_ties).alias("premium_dense_rank"),
        F.round(F.percent_rank().over(w_status_group_status_by_start), 3).alias("start_date_percent_rank"),
        F.count(F.lit(1)).over(w_status_group).alias("rows_in_status_group"),
        (F.col("premium") - F.lag("premium").over(w_status_by_premium)).alias("premium_change"),
        F.lead("start_date", 3).over(w_all_by_premium).alias("next3_start_date"),
        F.ntile(3).over(w_all_by_premium).alias("premium_tercile"),
        F.nth_value("premium", 2).over(w_all_by_premium_full).alias("second_premium"),
        F.round(F.col("premium") - F.avg("premium").over(w_status_group), 2).alias("premium_vs_status_group_avg"),
        F.dense_rank().over(w_all_by_premium_ties).alias("premium_dense_rank_2"),
        F.lead("premium", 1, 0).over(w_all_by_premium).alias("next_premium"),
        F.rank().over(w_all_by_premium_ties).alias("premium_rank"),
        F.last("premium", ignorenulls=True).over(w_product_by_start_date_runnin).alias("last_known_premium"),
        F.sum("premium").over(w_status_group_holder_id).alias("status_group_holder_id_sum_premium"),
        F.count(F.lit(1)).over(w_all_by_premium_running).alias("running_count"),
    )
)

sequenced = (
    with_trends
    .select(
        "*",
        F.row_number().over(w_holder_id_by_premium_change).alias("rn"),
        F.lag("last_known_premium", 3).over(w_all_by_rows_in_status_group).alias("prev3_last_known_premium"),
        F.nth_value("next3_start_date", 2).over(w_holder_id_by_premium_change_2).alias("second_next3_start_date"),
        F.count(F.lit(1)).over(w_next3_start_date_year).alias("rows_in_next3_start_date_year"),
        F.rank().over(w_all_by_rows_in_status_group_2).alias("rows_in_status_group_rank"),
        F.count(F.lit(1)).over(w_premium_tercile_status_by_pr).alias("running_count_2"),
        F.round(F.avg("running_count").over(w_next3_start_date_year), 2).alias("next3_start_date_year_avg_running_count"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "holder_id",
        "premium",
        "status",
        "premium_dense_rank",
        "premium_change",
        "next3_start_date",
        "premium_tercile",
        "second_premium",
        "premium_vs_status_group_avg",
        "premium_dense_rank_2",
        "next_premium",
        "premium_rank",
        "status_group_holder_id_sum_premium",
        "rn",
        "second_next3_start_date",
        "rows_in_next3_start_date_year",
        "rows_in_status_group_rank",
        "running_count_2",
        "next3_start_date_year_avg_running_count",
        F.length(F.trim(F.col("status_group"))).alias("status_group_len"),
    )
    .orderBy(F.col("premium_dense_rank_2").desc())
)
