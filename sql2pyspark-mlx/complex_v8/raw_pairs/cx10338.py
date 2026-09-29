from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_holder_id = Window.partitionBy("status", "holder_id")
w_status_by_end_date_full = Window.partitionBy("status").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_by_end_date_ties = Window.partitionBy("status").orderBy("end_date")
w_holder_id_by_premium = Window.partitionBy("holder_id").orderBy(F.col("premium").desc_nulls_first(), "id")
w_status_end_date_year_by_prem = Window.partitionBy("status", F.year("end_date")).orderBy("premium", "id").rowsBetween(-3, Window.currentRow)
w_status_by_end_date = Window.partitionBy("status").orderBy("end_date", "id")
w_status_by_end_date_running = Window.partitionBy("status").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_end_date_year_by_prem_2 = Window.partitionBy("status", F.year("end_date")).orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_id_by_premium_full = Window.partitionBy("holder_id").orderBy(F.col("premium").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_end_date_decile = Window.partitionBy("end_date_decile")
w_all_by_end_date_decile = Window.orderBy("end_date_decile", "id")
w_end_date_month_end_date_deci = Window.partitionBy(F.date_trunc("month", "end_date"), "end_date_decile")
w_end_date_year_status_by_prem = Window.partitionBy(F.year("end_date"), "status").orderBy("premium_vs_status_holder_id_avg")
w_all_by_premium_change = Window.orderBy("premium_change", "id")

base_policys = (
    df_policies
    .filter(F.col("end_date") >= F.lit("2026-02-16").cast("date"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        "status",
    )
)

with_trends = (
    base_policys
    .select(
        "*",
        F.round(F.col("premium") - F.avg("premium").over(w_status_holder_id), 2).alias("premium_vs_status_holder_id_avg"),
        F.size(F.collect_set("product").over(w_status_holder_id)).alias("distinct_product_in_status_holder_id"),
        F.nth_value("premium", 2).over(w_status_by_end_date_full).alias("second_premium"),
        F.dense_rank().over(w_status_by_end_date_ties).alias("end_date_dense_rank"),
        (F.col("premium") - F.lag("premium").over(w_holder_id_by_premium)).alias("premium_change"),
        F.round(F.avg("premium").over(w_status_end_date_year_by_prem), 2).alias("premium_avg_4"),
        F.ntile(10).over(w_status_by_end_date).alias("end_date_decile"),
        F.max("premium").over(w_status_by_end_date_running).alias("max_premium_so_far"),
        F.sum("premium").over(w_status_end_date_year_by_prem_2).alias("running_premium"),
        F.row_number().over(w_holder_id_by_premium).alias("seq_no"),
        F.last("premium").over(w_holder_id_by_premium_full).alias("last_premium"),
        F.count(F.lit(1)).over(w_status_holder_id).alias("rows_in_status_holder_id"),
    )
)

top_rows = (
    with_trends
    .filter(F.col("seq_no") <= 5)
    .drop("seq_no")
)

metrics = (
    top_rows
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "status",
        "premium_vs_status_holder_id_avg",
        "distinct_product_in_status_holder_id",
        "second_premium",
        "end_date_decile",
        "max_premium_so_far",
        "last_premium",
        "rows_in_status_holder_id",
        F.size(F.collect_set("holder_id").over(w_end_date_decile)).alias("distinct_holder_id_in_end_date_decile"),
        F.ntile(3).over(w_all_by_end_date_decile).alias("end_date_decile_tercile"),
        F.lead("premium_avg_4").over(w_all_by_end_date_decile).alias("next_premium_avg_4"),
        F.count(F.lit(1)).over(w_end_date_month_end_date_deci).alias("rows_in_end_date_month_end_date_decile"),
        F.dense_rank().over(w_end_date_year_status_by_prem).alias("premium_vs_status_holder_id_avg_dense_rank"),
        F.dense_rank().over(w_all_by_end_date_decile).alias("end_date_decile_dense_rank"),
        F.size(F.collect_set("status").over(w_end_date_decile)).alias("distinct_status_in_end_date_decile"),
        F.rank().over(w_all_by_premium_change).alias("premium_change_rank"),
    )
)

result = (
    metrics
    .select(
        "id",
        "product",
        "status",
        "end_date_decile",
        "max_premium_so_far",
        "last_premium",
        "rows_in_status_holder_id",
        "distinct_holder_id_in_end_date_decile",
        "end_date_decile_tercile",
        "rows_in_end_date_month_end_date_decile",
    )
    .orderBy(
        F.col("rows_in_end_date_month_end_date_decile").desc(),
        F.col("id").desc(),
    )
)
