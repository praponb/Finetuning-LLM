from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_by_premium = Window.partitionBy("status").orderBy("premium", "id")
w_end_date_month = Window.partitionBy(F.month("end_date"))
w_end_date_month_by_end_date_f = Window.partitionBy(F.date_trunc("month", "end_date")).orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_id = Window.partitionBy("holder_id")
w_all_by_end_date = Window.orderBy(F.col("end_date").desc_nulls_first(), "id")
w_end_date_month_by_end_date = Window.partitionBy(F.date_trunc("month", "end_date")).orderBy("end_date", "id")
w_end_date_month_by_end_date_t = Window.partitionBy(F.date_trunc("month", "end_date")).orderBy("end_date")

prepared_policys = (
    df_policies
    .filter((F.col("premium") <= 3521.8) & (F.col("start_date") >= F.lit("2024-10-22").cast("date")))
    .select(
        "id",
        "holder_id",
        "premium",
        "end_date",
        "status",
    )
)

windowed = (
    prepared_policys
    .select(
        "id",
        "holder_id",
        "premium",
        "end_date",
        F.rank().over(w_status_by_premium).alias("premium_rank"),
        F.sum("premium").over(w_end_date_month).alias("end_date_month_sum_premium"),
        F.first("premium").over(w_end_date_month_by_end_date_f).alias("first_premium"),
        F.min("premium").over(w_holder_id).alias("holder_id_min_premium"),
        F.sum("premium").over(w_holder_id).alias("holder_id_sum_premium"),
        F.lag("premium", 2).over(w_all_by_end_date).alias("prev2_premium"),
        F.lead("premium").over(w_end_date_month_by_end_date).alias("next_premium"),
        F.rank().over(w_end_date_month_by_end_date_t).alias("end_date_rank"),
        F.lag("end_date").over(w_status_by_premium).alias("prev_end_date"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_holder_id), F.lit(0)), 4).alias("premium_share_of_holder_id"),
        F.max("premium").over(w_holder_id).alias("holder_id_max_premium"),
        F.round(F.col("premium") - F.avg("premium").over(w_end_date_month), 2).alias("premium_vs_end_date_month_avg"),
    )
)

top_rows = (
    windowed
    .filter(F.col("end_date_rank") == 1)
    .drop("end_date_rank")
)

totals = (
    top_rows
    .filter(F.col("first_premium") < 4417.0)
    .groupBy(
        "premium_rank",
        F.date_format("end_date", "yyyy-MM").alias("end_date_ym"),
    )
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("holder_id_sum_premium").alias("total_holder_id_sum_premium"),
        F.percentile_approx("prev2_premium", 0.5).alias("median_prev2_premium"),
        F.max("end_date").alias("last_end_date"),
        F.round(F.sum("prev2_premium"), 2).alias("total_prev2_premium"),
    )
    .filter(F.col("n_rows") >= 3)
)

result = (
    totals
    .filter(F.col("total_prev2_premium").between(3180.0, 21060.0))
    .select(
        "premium_rank",
        "end_date_ym",
        "n_rows",
        "total_holder_id_sum_premium",
        "median_prev2_premium",
        "last_end_date",
        "total_prev2_premium",
        F.date_format(F.col("last_end_date"), "yyyy-MM-dd").alias("last_end_date_label"),
        F.abs(F.col("n_rows") - 8).alias("n_rows_distance"),
    )
    .orderBy("last_end_date_label", F.col("n_rows_distance").asc_nulls_last())
)
