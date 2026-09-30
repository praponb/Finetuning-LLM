from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id = Window.partitionBy("holder_id")
w_status_holder_id_by_start_da = Window.partitionBy("status", "holder_id").orderBy(F.col("start_date").asc_nulls_last(), "id")
w_status_holder_id_by_start_da_2 = Window.partitionBy("status", "holder_id").orderBy(F.col("start_date").asc_nulls_last())
w_status_start_date_month = Window.partitionBy("status", F.date_trunc("month", "start_date"))
w_start_date_month_by_end_date = Window.partitionBy(F.date_trunc("month", "start_date")).orderBy(F.col("end_date").asc_nulls_last())
w_start_date_month_by_end_date_2 = Window.partitionBy(F.date_trunc("month", "start_date")).orderBy(F.col("end_date").asc_nulls_last(), "id").rowsBetween(-3, -1)
w_status_holder_id_by_start_da_3 = Window.partitionBy("status", "holder_id").orderBy(F.col("start_date").asc_nulls_last(), "id").rowsBetween(-6, Window.currentRow)
w_start_date_month_by_end_date_3 = Window.partitionBy(F.date_trunc("month", "start_date")).orderBy(F.col("end_date").asc_nulls_last(), "id").rowsBetween(-6, Window.currentRow)
w_start_date_month_by_end_date_4 = Window.partitionBy(F.date_trunc("month", "start_date")).orderBy(F.col("end_date").asc_nulls_last(), "id").rowsBetween(-2, Window.currentRow)
w_status_by_start_date_running = Window.partitionBy("status").orderBy(F.col("start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_last_known_premium = Window.orderBy("last_known_premium", "id")
w_start_date_rank = Window.partitionBy("start_date_rank")

scoped_policys = (
    df_policies
    .filter((F.year(F.col("start_date")) == 2025) & (F.col("status").isin("ACTIVE", "LAPSED")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.round(F.col("premium") * 0.25, 2).alias("premium_adj"),
        F.date_format(F.col("end_date"), "yyyy-MM-dd").alias("end_date_label"),
    )
)

metrics = (
    scoped_policys
    .select(
        "*",
        F.min("premium").over(w_holder_id).alias("holder_id_min_premium"),
        F.round(F.percent_rank().over(w_status_holder_id_by_start_da), 3).alias("start_date_percent_rank"),
        F.rank().over(w_status_holder_id_by_start_da_2).alias("start_date_rank"),
        F.max("premium").over(w_status_start_date_month).alias("status_start_date_month_max_premium"),
        F.round(F.col("premium_adj") - F.avg("premium_adj").over(w_status_start_date_month), 2).alias("premium_adj_vs_status_start_date_month_avg"),
        F.round(F.col("premium_adj") / F.nullif(F.sum("premium_adj").over(w_holder_id), F.lit(0)), 4).alias("premium_adj_share_of_holder_id"),
        F.round(F.col("premium_adj") - F.avg("premium_adj").over(w_holder_id), 2).alias("premium_adj_vs_holder_id_avg"),
        F.rank().over(w_start_date_month_by_end_date).alias("end_date_rank"),
        F.max("premium").over(w_holder_id).alias("holder_id_max_premium"),
        F.round(F.avg("premium_adj").over(w_start_date_month_by_end_date_2), 2).alias("premium_adj_avg_3_prior"),
        F.round(F.avg("premium_adj").over(w_status_holder_id_by_start_da_3), 2).alias("premium_adj_avg_7"),
        F.round(F.col("premium") - F.avg("premium").over(w_status_start_date_month), 2).alias("premium_vs_status_start_date_month_avg"),
        F.round(F.avg("premium").over(w_start_date_month_by_end_date_3), 2).alias("premium_avg_7"),
        F.max("premium_adj").over(w_start_date_month_by_end_date_4).alias("premium_adj_max_3"),
        F.last("premium", ignorenulls=True).over(w_status_by_start_date_running).alias("last_known_premium"),
    )
)

windowed = (
    metrics
    .select(
        "*",
        F.round(F.percent_rank().over(w_all_by_last_known_premium), 3).alias("last_known_premium_percent_rank"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_holder_id), F.lit(0)), 4).alias("premium_share_of_holder_id"),
        F.max("premium_adj_avg_7").over(w_start_date_rank).alias("start_date_rank_max_premium_adj_avg_7"),
        F.round(F.col("premium_adj_vs_holder_id_avg") / F.nullif(F.sum("premium_adj_vs_holder_id_avg").over(w_start_date_rank), F.lit(0)), 4).alias("premium_adj_vs_holder_id_avg_share_of_start_date"),
    )
)

result = (
    windowed
    .select(
        "id",
        "holder_id",
        "premium",
        "end_date",
        "status",
        "holder_id_min_premium",
        "status_start_date_month_max_premium",
        "premium_adj_vs_status_start_date_month_avg",
        "premium_adj_vs_holder_id_avg",
        "holder_id_max_premium",
        "premium_adj_avg_7",
        "premium_vs_status_start_date_month_avg",
        "premium_adj_max_3",
        "last_known_premium",
        "last_known_premium_percent_rank",
        "premium_share_of_holder_id",
        "start_date_rank_max_premium_adj_avg_7",
        "premium_adj_vs_holder_id_avg_share_of_start_date",
        F.initcap(F.trim(F.col("status"))).alias("status_clean"),
        F.date_format(F.col("end_date"), "MMM yyyy").alias("end_date_label_2"),
    )
)
