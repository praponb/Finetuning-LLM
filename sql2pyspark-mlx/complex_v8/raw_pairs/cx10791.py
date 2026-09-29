from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_end_date_month = Window.partitionBy("holder_id", F.month("end_date"))
w_end_date_year_by_end_date = Window.partitionBy(F.year("end_date")).orderBy(F.col("end_date").desc_nulls_first(), "id")
w_all_by_premium_vs_holder_id = Window.orderBy("premium_vs_holder_id_end_date_month_avg", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_premium_vs_holder_id_2 = Window.orderBy("premium_vs_holder_id_end_date_month_avg", "id")

base = (
    df_policies
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket"),
    )
)

with_windows = (
    base
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "premium_bucket",
        F.round(F.col("premium") - F.avg("premium").over(w_holder_id_end_date_month), 2).alias("premium_vs_holder_id_end_date_month_avg"),
        F.lag("premium", 1, 0).over(w_end_date_year_by_end_date).alias("prev_premium"),
    )
)

with_windows_2 = (
    with_windows
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "premium_bucket",
        "prev_premium",
        F.first("premium_bucket").over(w_all_by_premium_vs_holder_id).alias("first_premium_bucket"),
        F.round(F.percent_rank().over(w_all_by_premium_vs_holder_id_2), 3).alias("premium_vs_holder_id_end_date_month_avg_percent_"),
    )
)

result = (
    with_windows_2
    .filter(F.col("premium_vs_holder_id_end_date_month_avg_percent_") >= 0.3)
    .select(
        "id",
        "holder_id",
        "premium",
        "end_date",
        "premium_bucket",
        "prev_premium",
        "first_premium_bucket",
        "premium_vs_holder_id_end_date_month_avg_percent_",
        F.abs(F.col("first_premium_bucket") - 3560).alias("first_premium_bucket_distance"),
    )
    .orderBy(F.col("premium").desc(), "id")
    .limit(20)
)
