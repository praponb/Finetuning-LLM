from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_end_date = Window.orderBy(F.col("end_date").desc(), "id")
w_all_by_end_date_month_end_ti = Window.orderBy("end_date_month_end")
w_start_date_month_by_end_date = Window.partitionBy(F.month("start_date")).orderBy(F.col("end_date").desc(), "id")
w_all_by_start_date_ties = Window.orderBy("start_date")
w_all_by_end_date_month_end_fu = Window.orderBy("end_date_month_end", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_start_date = Window.orderBy("start_date", "id")
w_all_by_end_date_month_end = Window.orderBy("end_date_month_end", "id")
w_all_by_start_date_full = Window.orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_end_date_month_end_month_by = Window.partitionBy(F.date_trunc("month", "end_date_month_end")).orderBy(F.col("next_start_date_2").desc(), "id")
w_second_end_date_year = Window.partitionBy(F.year("second_end_date"))
w_next_start_date_2_month_star = Window.partitionBy(F.month("next_start_date_2"), "start_date_rank").orderBy(F.col("end_date_month_end_rank").desc(), "id")

base_policys = (
    df_policies
    .filter(F.col("product") == "TRAVEL")
    .select(
        "id",
        "holder_id",
        "start_date",
        "end_date",
        "status",
        F.lower(F.regexp_replace(F.trim(F.col("product")), " ", "_")).alias("product_slug"),
        F.last_day(F.col("end_date")).alias("end_date_month_end"),
    )
)

metrics = (
    base_policys
    .select(
        "id",
        "start_date",
        "product_slug",
        "end_date_month_end",
        F.datediff(F.col("start_date"), F.lag("start_date").over(w_all_by_end_date)).alias("days_since_prev_start_date"),
        F.rank().over(w_all_by_end_date_month_end_ti).alias("end_date_month_end_rank"),
        F.datediff(F.col("end_date_month_end"), F.lag("end_date_month_end").over(w_start_date_month_by_end_date)).alias("days_since_prev_end_date_month_end"),
        F.round(F.cume_dist().over(w_all_by_start_date_ties), 3).alias("start_date_cume_dist"),
        F.nth_value("end_date", 2).over(w_all_by_end_date_month_end_fu).alias("second_end_date"),
        F.round(F.percent_rank().over(w_all_by_start_date), 3).alias("start_date_percent_rank"),
        F.lead("start_date").over(w_all_by_end_date_month_end).alias("next_start_date"),
        F.datediff(F.col("end_date"), F.lag("end_date").over(w_all_by_end_date)).alias("days_since_prev_end_date"),
        F.nth_value("start_date", 2).over(w_all_by_start_date_full).alias("second_start_date"),
        F.lead("start_date").over(w_all_by_end_date).alias("next_start_date_2"),
        F.rank().over(w_all_by_start_date_ties).alias("start_date_rank"),
        F.ntile(4).over(w_start_date_month_by_end_date).alias("end_date_quartile"),
    )
)

annotated = (
    metrics
    .filter(F.col("end_date_month_end_rank") >= 2)
    .select(
        "*",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("end_date_month_end")).alias("days_since_end_date_month_end"),
    )
)

metrics_2 = (
    annotated
    .select(
        "*",
        (F.col("end_date_quartile") - F.lag("end_date_quartile").over(w_end_date_month_end_month_by)).alias("end_date_quartile_change"),
        F.max("end_date_quartile").over(w_second_end_date_year).alias("second_end_date_year_max_end_date_quartile"),
        F.count(F.lit(1)).over(w_second_end_date_year).alias("rows_in_second_end_date_year"),
        F.round(F.percent_rank().over(w_end_date_month_end_month_by), 3).alias("next_start_date_2_percent_rank"),
        F.round(F.avg("start_date_percent_rank").over(w_second_end_date_year), 2).alias("second_end_date_year_avg_start_date_percent_rank"),
        F.dense_rank().over(w_end_date_month_end_month_by).alias("next_start_date_2_dense_rank"),
        F.lead("start_date_percent_rank", 3).over(w_next_start_date_2_month_star).alias("next3_start_date_percent_rank"),
        F.rank().over(w_next_start_date_2_month_star).alias("end_date_month_end_rank_rank"),
    )
)

result = (
    metrics_2
    .filter(F.col("days_since_prev_end_date_month_end").between(40, 130))
    .select(
        "id",
        "start_date",
        "product_slug",
        "end_date_month_end",
        "days_since_prev_start_date",
        "end_date_month_end_rank",
        "days_since_prev_end_date_month_end",
        "start_date_cume_dist",
        "next_start_date",
        "second_start_date",
        "next_start_date_2",
        "end_date_quartile",
        "days_since_end_date_month_end",
        "second_end_date_year_max_end_date_quartile",
        "rows_in_second_end_date_year",
        "next_start_date_2_percent_rank",
        "next_start_date_2_dense_rank",
    )
    .orderBy("second_start_date", F.col("next_start_date_2").desc_nulls_first())
)
