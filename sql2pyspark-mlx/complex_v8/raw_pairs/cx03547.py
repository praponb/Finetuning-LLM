from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_holder_id_by_start_da = Window.partitionBy("status", "holder_id").orderBy("start_date", "id").rowsBetween(-2, Window.currentRow)
w_status_by_premium_distance = Window.partitionBy("status").orderBy(F.col("premium_distance").desc_nulls_first(), "id")
w_status = Window.partitionBy("status")
w_start_date_year_status_by_st = Window.partitionBy(F.year("start_date"), "status").orderBy(F.col("start_date").desc(), "id")
w_status_holder_id_by_premium = Window.partitionBy("status", "holder_id").orderBy(F.col("premium_distance").desc(), "id").rowsBetween(-2, -1)
w_status_holder_id_by_start_da_2 = Window.partitionBy("status", "holder_id").orderBy("start_date", "id")
w_start_date_year_status_by_st_2 = Window.partitionBy(F.year("start_date"), "status").orderBy(F.col("start_date").desc())
w_status_holder_id_by_premium_2 = Window.partitionBy("status", "holder_id").orderBy(F.col("premium_adj").asc_nulls_last(), "id")
w_all_by_next2_premium_adj_tie = Window.orderBy("next2_premium_adj")
w_start_date_tercile_by_start = Window.partitionBy("start_date_tercile").orderBy(F.col("start_date_tercile").asc_nulls_last(), "id")
w_all_by_next2_premium_adj = Window.orderBy("next2_premium_adj", "id")
w_start_date_tercile_by_start_2 = Window.partitionBy("start_date_tercile").orderBy(F.col("start_date_tercile").asc_nulls_last())
w_all_by_next2_premium_adj_run = Window.orderBy("next2_premium_adj", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_id_status = Window.partitionBy("holder_id", "status")

cleaned = (
    df_policies
    .filter(~F.col("status").isin("EXPIRED", "ACTIVE"))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "status",
        F.round(F.col("premium") * 0.9, 2).alias("premium_adj"),
        F.abs(F.col("premium") - 2824.5).alias("premium_distance"),
    )
)

windowed = (
    cleaned
    .select(
        "*",
        F.round(F.avg("premium").over(w_status_holder_id_by_start_da), 2).alias("premium_avg_3"),
        F.datediff(F.col("start_date"), F.lag("start_date").over(w_status_by_premium_distance)).alias("days_since_prev_start_date"),
        F.sum("premium_adj").over(w_status).alias("status_sum_premium_adj"),
        F.count(F.lit(1)).over(w_status).alias("rows_in_status"),
        F.row_number().over(w_status_by_premium_distance).alias("premium_distance_seq"),
        F.round(F.percent_rank().over(w_start_date_year_status_by_st), 3).alias("start_date_percent_rank"),
        F.max("premium_adj").over(w_status_holder_id_by_premium).alias("premium_adj_max_2_prior"),
        F.ntile(3).over(w_status_holder_id_by_start_da_2).alias("start_date_tercile"),
        F.round(F.cume_dist().over(w_start_date_year_status_by_st_2), 3).alias("start_date_cume_dist"),
        F.lag("premium_distance").over(w_status_holder_id_by_start_da_2).alias("prev_premium_distance"),
        F.lead("premium_adj", 2).over(w_status_holder_id_by_premium_2).alias("next2_premium_adj"),
        F.lag("premium").over(w_status_holder_id_by_start_da_2).alias("prev_premium"),
    )
)

top_per_group = (
    windowed
    .filter((F.col("premium_distance_seq") == 1) & (F.col("start_date").isNotNull()))
    .drop("premium_distance_seq")
)

ranked = (
    top_per_group
    .select(
        "id",
        "premium",
        "start_date",
        "premium_adj",
        "premium_avg_3",
        "start_date_tercile",
        "start_date_cume_dist",
        "prev_premium_distance",
        "next2_premium_adj",
        "prev_premium",
        F.round(F.cume_dist().over(w_all_by_next2_premium_adj_tie), 3).alias("next2_premium_adj_cume_dist"),
        F.round(F.percent_rank().over(w_start_date_tercile_by_start), 3).alias("start_date_tercile_percent_rank"),
        F.lead("next2_premium_adj").over(w_all_by_next2_premium_adj).alias("next_next2_premium_adj"),
        F.round(F.cume_dist().over(w_start_date_tercile_by_start_2), 3).alias("start_date_tercile_cume_dist"),
        F.max("next2_premium_adj").over(w_all_by_next2_premium_adj_run).alias("max_next2_premium_adj_so_far"),
        F.round(F.col("prev_premium_distance") - F.avg("prev_premium_distance").over(w_holder_id_status), 2).alias("prev_premium_distance_vs_holder_id_status_avg"),
        F.rank().over(w_all_by_next2_premium_adj_tie).alias("next2_premium_adj_rank"),
    )
)

result = (
    ranked
    .filter(F.col("next2_premium_adj") <= 5310.0)
    .select(
        "id",
        "premium",
        "premium_adj",
        "premium_avg_3",
        "start_date_tercile",
        "prev_premium",
        "start_date_tercile_percent_rank",
        "start_date_tercile_cume_dist",
        F.trunc(F.col("start_date"), "MM").alias("start_date_month_start"),
        (F.col("premium_avg_3") + F.col("start_date_cume_dist")).alias("premium_avg_3_plus_start_date_cume_dist"),
    )
    .orderBy(
        "premium_avg_3_plus_start_date_cume_dist",
        F.col("start_date_tercile_cume_dist").desc(),
    )
)
