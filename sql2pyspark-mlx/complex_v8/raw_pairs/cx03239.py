from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_by_start_date_full = Window.partitionBy("holder_id").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_start_date_year_holder_id_by = Window.partitionBy(F.year("start_date"), "holder_id").orderBy(F.col("end_date").desc(), "id")
w_all_by_end_date = Window.orderBy("end_date", "id")
w_all_by_start_date = Window.orderBy(F.col("start_date").desc(), "id")
w_holder_id_by_start_date_movi = Window.partitionBy("holder_id").orderBy("start_date", "id").rowsBetween(-6, Window.currentRow)
w_end_date_month_by_end_date_f = Window.partitionBy(F.month("end_date")).orderBy(F.col("end_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_end_date_month_by_end_date = Window.partitionBy(F.month("end_date")).orderBy(F.col("end_date").desc(), "id")
w_holder_id_by_start_date = Window.partitionBy("holder_id").orderBy("start_date", "id")
w_holder_id = Window.partitionBy("holder_id")
w_all_by_end_date_ties = Window.orderBy("end_date")
w_start_date_year_holder_id_by_2 = Window.partitionBy(F.year("start_date"), "holder_id").orderBy(F.col("end_date").desc(), "id").rowsBetween(-2, Window.currentRow)
w_product_or_unknown = Window.partitionBy("product_or_unknown")
w_end_date_year = Window.partitionBy(F.year("end_date"))
w_end_date_rank_second_end_dat = Window.partitionBy("end_date_rank", F.year("second_end_date")).orderBy("seq_no_2", "id")
w_prev_start_date_month_start = Window.partitionBy(F.month("prev_start_date"), F.year("start_date")).orderBy("end_date")
w_end_date_rank_by_second_end = Window.partitionBy("end_date_rank").orderBy(F.col("second_end_date").desc(), "id").rowsBetween(-2, Window.currentRow)

valid = (
    df_policies
    .filter((F.col("holder_id").isNotNull()) & (F.col("status").isNotNull()))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        F.coalesce(F.col("product"), F.lit("UNKNOWN")).alias("product_or_unknown"),
        F.substring(F.trim(F.col("product")), 1, 4).alias("product_prefix"),
    )
)

ranked = (
    valid
    .select(
        "*",
        F.nth_value("end_date", 2).over(w_holder_id_by_start_date_full).alias("second_end_date"),
        F.rank().over(w_start_date_year_holder_id_by).alias("end_date_rank"),
        F.round(F.percent_rank().over(w_all_by_end_date), 3).alias("end_date_percent_rank"),
        F.lag("end_date", 3).over(w_all_by_start_date).alias("prev3_end_date"),
        F.sum("premium").over(w_holder_id_by_start_date_movi).alias("premium_sum_7"),
        F.nth_value("end_date", 2).over(w_end_date_month_by_end_date_f).alias("second_end_date_2"),
        F.lag("start_date").over(w_end_date_month_by_end_date).alias("prev_start_date"),
        F.row_number().over(w_holder_id_by_start_date).alias("seq_no"),
        F.round(F.col("premium") - F.avg("premium").over(w_holder_id), 2).alias("premium_vs_holder_id_avg"),
        F.round(F.cume_dist().over(w_start_date_year_holder_id_by), 3).alias("end_date_cume_dist"),
        F.round(F.percent_rank().over(w_all_by_end_date_ties), 3).alias("end_date_percent_rank_2"),
        F.max("premium").over(w_start_date_year_holder_id_by_2).alias("premium_max_3"),
        F.round(F.col("premium") - F.avg("premium").over(w_product_or_unknown), 2).alias("premium_vs_product_or_unknown_avg"),
        F.row_number().over(w_start_date_year_holder_id_by).alias("seq_no_2"),
        F.round(F.cume_dist().over(w_holder_id_by_start_date), 3).alias("start_date_cume_dist"),
    )
)

sequenced = (
    ranked
    .select(
        "*",
        F.count(F.lit(1)).over(w_end_date_year).alias("rows_in_end_date_year"),
        F.round(F.percent_rank().over(w_end_date_rank_second_end_dat), 3).alias("seq_no_2_percent_rank"),
        F.dense_rank().over(w_prev_start_date_month_start).alias("end_date_dense_rank"),
        F.round(F.col("end_date_percent_rank") - F.avg("end_date_percent_rank").over(w_end_date_year), 2).alias("end_date_percent_rank_vs_end_date_year_avg"),
        F.sum("seq_no").over(w_end_date_rank_by_second_end).alias("seq_no_sum_3"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "product_or_unknown",
        "second_end_date",
        "end_date_rank",
        "end_date_percent_rank",
        "prev3_end_date",
        "premium_sum_7",
        "prev_start_date",
        "seq_no",
        "end_date_percent_rank_2",
        "premium_max_3",
        "seq_no_2",
        "start_date_cume_dist",
        "rows_in_end_date_year",
        "seq_no_2_percent_rank",
        "end_date_dense_rank",
        "end_date_percent_rank_vs_end_date_year_avg",
    )
    .orderBy(F.col("seq_no_2").asc(), "id")
    .limit(20)
)
