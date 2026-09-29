from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_product_by_premium_pr = Window.partitionBy("status", "product").orderBy(F.col("premium").desc(), "id").rowsBetween(-2, -1)
w_start_date_month_end_month_b = Window.partitionBy(F.month("start_date_month_end")).orderBy("status_len", "id").rowsBetween(-6, -1)
w_holder_id_start_date_month_e = Window.partitionBy("holder_id", F.month("start_date_month_end")).orderBy(F.col("start_date").asc_nulls_last())
w_status_len_holder_id = Window.partitionBy("status_len", "holder_id")
w_status_product_by_premium = Window.partitionBy("status", "product").orderBy(F.col("premium").desc(), "id")
w_start_date_year = Window.partitionBy(F.year("start_date"))
w_start_date_month_end_month_p = Window.partitionBy(F.date_trunc("month", "start_date_month_end"), "product").orderBy("start_date", "id")
w_status_len_holder_id_by_prem = Window.partitionBy("status_len", "holder_id").orderBy("premium", "id")
w_holder_id_start_date_month_e_2 = Window.partitionBy("holder_id", F.month("start_date_month_end")).orderBy(F.col("start_date").asc_nulls_last(), "id")
w_premium_tercile = Window.partitionBy("premium_tercile")
w_all_by_start_date_cume_dist = Window.orderBy("start_date_cume_dist", "id").rowsBetween(-6, Window.currentRow)
w_all_by_start_date_cume_dist_2 = Window.orderBy("start_date_cume_dist")

prepared_policys = (
    df_policies
    .filter((F.col("status") == "EXPIRED") | (F.year(F.col("end_date")) == 2026))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.length(F.trim(F.col("status"))).alias("status_len"),
        F.last_day(F.col("start_date")).alias("start_date_month_end"),
    )
)

windowed = (
    prepared_policys
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "status",
        "status_len",
        "start_date_month_end",
        F.max("status_len").over(w_status_product_by_premium_pr).alias("status_len_max_2_prior"),
        F.max("premium").over(w_start_date_month_end_month_b).alias("premium_max_6_prior"),
        F.dense_rank().over(w_holder_id_start_date_month_e).alias("start_date_dense_rank"),
        F.round(F.col("premium") - F.avg("premium").over(w_status_len_holder_id), 2).alias("premium_vs_status_len_holder_id_avg"),
        F.datediff(F.col("start_date"), F.lag("start_date").over(w_status_product_by_premium)).alias("days_since_prev_start_date"),
        F.count(F.lit(1)).over(w_start_date_year).alias("rows_in_start_date_year"),
        F.lead("start_date").over(w_start_date_month_end_month_p).alias("next_start_date"),
        F.size(F.collect_set("status").over(w_start_date_year)).alias("distinct_status_in_start_date_year"),
        F.sum("premium").over(w_start_date_year).alias("start_date_year_sum_premium"),
        F.ntile(3).over(w_status_len_holder_id_by_prem).alias("premium_tercile"),
        F.round(F.cume_dist().over(w_holder_id_start_date_month_e_2), 3).alias("start_date_cume_dist"),
    )
)

sequenced = (
    windowed
    .select(
        "id",
        "holder_id",
        "premium",
        "status",
        "status_len",
        "premium_max_6_prior",
        "start_date_dense_rank",
        "days_since_prev_start_date",
        "rows_in_start_date_year",
        "next_start_date",
        "start_date_year_sum_premium",
        "start_date_cume_dist",
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_premium_tercile), F.lit(0)), 4).alias("premium_share_of_premium_tercile"),
        F.round(F.avg("premium").over(w_all_by_start_date_cume_dist), 2).alias("premium_avg_7"),
        F.round(F.col("start_date_dense_rank") / F.nullif(F.sum("start_date_dense_rank").over(w_premium_tercile), F.lit(0)), 4).alias("start_date_dense_rank_share_of_premium_tercile"),
        F.min("start_date_cume_dist").over(w_premium_tercile).alias("premium_tercile_min_start_date_cume_dist"),
        F.size(F.collect_set("status").over(w_premium_tercile)).alias("distinct_status_in_premium_tercile"),
        F.min("start_date_year_sum_premium").over(w_premium_tercile).alias("premium_tercile_min_start_date_year_sum_premium"),
        F.round(F.percent_rank().over(w_all_by_start_date_cume_dist_2), 3).alias("start_date_cume_dist_percent_rank"),
        F.max("start_date_cume_dist").over(w_premium_tercile).alias("premium_tercile_max_start_date_cume_dist"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "holder_id",
        "premium",
        "status",
        "status_len",
        "start_date_dense_rank",
        "days_since_prev_start_date",
        "rows_in_start_date_year",
        "next_start_date",
        "start_date_year_sum_premium",
        "start_date_cume_dist",
        "premium_avg_7",
        "start_date_dense_rank_share_of_premium_tercile",
        "premium_tercile_min_start_date_cume_dist",
        "distinct_status_in_premium_tercile",
        "premium_tercile_min_start_date_year_sum_premium",
        "start_date_cume_dist_percent_rank",
        "premium_tercile_max_start_date_cume_dist",
    )
    .orderBy(F.col("start_date_cume_dist").asc_nulls_last(), "id")
    .limit(10)
)
