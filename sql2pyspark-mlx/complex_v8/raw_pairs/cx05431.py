from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_end_date_ties = Window.orderBy(F.col("end_date").desc())
w_end_date_month_by_premium_mo = Window.partitionBy(F.date_trunc("month", "end_date")).orderBy("premium", "id").rowsBetween(-3, Window.currentRow)
w_all_by_premium = Window.orderBy(F.col("premium").desc_nulls_first(), "id")
w_end_date_month = Window.partitionBy(F.month("end_date"))
w_end_date_month_holder_id_by = Window.partitionBy(F.month("end_date"), "holder_id").orderBy(F.col("premium").desc(), "id")
w_product = Window.partitionBy("product")
w_all_by_premium_prior_2 = Window.orderBy(F.col("premium").desc_nulls_first(), "id").rowsBetween(-2, -1)
w_all_by_premium_ties = Window.orderBy(F.col("premium").desc_nulls_first())
w_all_by_end_date = Window.orderBy(F.col("end_date").desc(), "id")
w_all_by_premium_prior_1 = Window.orderBy(F.col("premium").desc_nulls_first(), "id").rowsBetween(-1, -1)
w_end_date_month_by_premium = Window.partitionBy(F.date_trunc("month", "end_date")).orderBy("premium", "id")
w_all_by_premium_max_1_prior = Window.orderBy("premium_max_1_prior", "id")
w_premium_rank_by_days_end_dat = Window.partitionBy("premium_rank").orderBy("days_end_date_to_start_date_max_2_prior", "id")
w_premium_seq_end_date_rank = Window.partitionBy("premium_seq", "end_date_rank")
w_all_by_end_date_rank_running = Window.orderBy(F.col("end_date_rank").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent = (
    df_policies
    .filter((F.col("holder_id") >= 8) | (F.col("status").isNotNull()))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        F.datediff(F.col("start_date"), F.col("end_date")).alias("days_end_date_to_start_date"),
        F.concat_ws(": ", F.col("product"), F.col("status")).alias("product_status_label"),
    )
)

metrics = (
    recent
    .select(
        "id",
        "holder_id",
        "days_end_date_to_start_date",
        F.rank().over(w_all_by_end_date_ties).alias("end_date_rank"),
        F.round(F.avg("days_end_date_to_start_date").over(w_end_date_month_by_premium_mo), 2).alias("days_end_date_to_start_date_avg_4"),
        F.lead("premium").over(w_all_by_premium).alias("next_premium"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_end_date_month), F.lit(0)), 4).alias("premium_share_of_end_date_month"),
        F.row_number().over(w_end_date_month_holder_id_by).alias("premium_seq"),
        F.count(F.lit(1)).over(w_product).alias("rows_in_product"),
        F.round(F.cume_dist().over(w_all_by_end_date_ties), 3).alias("end_date_cume_dist"),
        F.max("days_end_date_to_start_date").over(w_all_by_premium_prior_2).alias("days_end_date_to_start_date_max_2_prior"),
        F.rank().over(w_all_by_premium_ties).alias("premium_rank"),
        F.round(F.percent_rank().over(w_all_by_premium_ties), 3).alias("premium_percent_rank"),
        F.lag("end_date", 3).over(w_all_by_end_date).alias("prev3_end_date"),
        F.round(F.col("days_end_date_to_start_date") - F.avg("days_end_date_to_start_date").over(w_product), 2).alias("days_end_date_to_start_date_vs_product_avg"),
        F.sum("premium").over(w_product).alias("product_sum_premium"),
        F.max("premium").over(w_all_by_premium_prior_1).alias("premium_max_1_prior"),
        (F.col("days_end_date_to_start_date") - F.lag("days_end_date_to_start_date").over(w_end_date_month_by_premium)).alias("days_end_date_to_start_date_change"),
    )
)

metrics_2 = (
    metrics
    .select(
        "id",
        "holder_id",
        "days_end_date_to_start_date",
        "end_date_rank",
        "next_premium",
        "premium_share_of_end_date_month",
        "premium_seq",
        "end_date_cume_dist",
        "days_end_date_to_start_date_max_2_prior",
        "premium_rank",
        "days_end_date_to_start_date_vs_product_avg",
        "premium_max_1_prior",
        "days_end_date_to_start_date_change",
        F.lead("end_date_rank", 2, 0).over(w_all_by_premium_max_1_prior).alias("next2_end_date_rank"),
        F.lead("end_date_rank", 3, 0).over(w_premium_rank_by_days_end_dat).alias("next3_end_date_rank"),
        F.count(F.lit(1)).over(w_premium_seq_end_date_rank).alias("rows_in_premium_seq_end_date_rank"),
        F.max("premium_seq").over(w_all_by_end_date_rank_running).alias("max_premium_seq_so_far"),
    )
)

result = (
    metrics_2
    .filter(F.col("max_premium_seq_so_far") > 26)
    .select(
        "id",
        "holder_id",
        "days_end_date_to_start_date",
        "end_date_rank",
        "next_premium",
        "premium_share_of_end_date_month",
        "premium_seq",
        "end_date_cume_dist",
        "days_end_date_to_start_date_max_2_prior",
        "premium_rank",
        "days_end_date_to_start_date_vs_product_avg",
        "premium_max_1_prior",
        "next2_end_date_rank",
        "next3_end_date_rank",
        "rows_in_premium_seq_end_date_rank",
        "max_premium_seq_so_far",
        (F.col("premium_rank") - F.col("max_premium_seq_so_far")).alias("premium_rank_minus_max_premium_seq_so_far"),
    )
    .orderBy(F.col("end_date_rank").desc())
)
