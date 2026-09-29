from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_end_date = Window.orderBy("end_date", "id")
w_all_by_end_date_full = Window.orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_end_date_quintile = Window.partitionBy("end_date_quintile")
w_end_date_quintile_by_next_en = Window.partitionBy("end_date_quintile").orderBy(F.col("next_end_date").desc(), "id").rowsBetween(-6, -1)
w_end_date_quintile_by_next_en_2 = Window.partitionBy("end_date_quintile").orderBy(F.col("next_end_date").desc(), "id")
w_end_date_quintile_by_next_en_3 = Window.partitionBy("end_date_quintile").orderBy(F.col("next_end_date").desc())
w_end_date_quintile_by_next_en_4 = Window.partitionBy("end_date_quintile").orderBy(F.col("next_end_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid = (
    df_policies
    .filter(F.col("status").isNotNull())
    .select(
        "id",
        "holder_id",
        "product",
        "end_date",
        "status",
    )
)

with_windows = (
    valid
    .select(
        "id",
        "end_date",
        F.round(F.percent_rank().over(w_all_by_end_date), 3).alias("end_date_percent_rank"),
        F.last("end_date").over(w_all_by_end_date_full).alias("last_end_date"),
        F.lead("end_date").over(w_all_by_end_date).alias("next_end_date"),
        F.ntile(5).over(w_all_by_end_date).alias("end_date_quintile"),
        F.round(F.cume_dist().over(w_all_by_end_date), 3).alias("end_date_cume_dist"),
    )
)

with_windows_2 = (
    with_windows
    .select(
        "*",
        F.max("end_date_cume_dist").over(w_end_date_quintile).alias("end_date_quintile_max_end_date_cume_dist"),
        F.round(F.avg("end_date_percent_rank").over(w_end_date_quintile_by_next_en), 2).alias("end_date_percent_rank_avg_6_prior"),
        F.datediff(F.col("last_end_date"), F.lag("last_end_date").over(w_end_date_quintile_by_next_en_2)).alias("days_since_prev_last_end_date"),
        F.dense_rank().over(w_end_date_quintile_by_next_en_3).alias("next_end_date_dense_rank"),
        F.first("end_date_cume_dist").over(w_end_date_quintile_by_next_en_4).alias("first_end_date_cume_dist"),
    )
)

result = (
    with_windows_2
    .select(
        "id",
        "end_date",
        "end_date_percent_rank",
        "next_end_date",
        "end_date_quintile",
        "end_date_quintile_max_end_date_cume_dist",
        "days_since_prev_last_end_date",
        F.abs(F.col("end_date_quintile") - 5).alias("end_date_quintile_distance"),
    )
    .orderBy(F.col("id").desc())
)
