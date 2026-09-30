from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_median_gpa_filled_dis = Window.orderBy(F.col("median_gpa_filled_distance").desc_nulls_first(), "is_international", "enrolled_on_ym")
w_is_international = Window.partitionBy("is_international")
w_all_by_median_gpa_filled_dis_2 = Window.orderBy(F.col("median_gpa_filled_distance").desc_nulls_first())

scoped_students = (
    df_students
    .select(
        "id",
        "student_name",
        "enrolled_on",
        "is_international",
        F.coalesce(F.col("gpa"), F.lit(0.0)).alias("gpa_filled"),
    )
)

summary = (
    scoped_students
    .groupBy(
        "is_international",
        F.date_format("enrolled_on", "yyyy-MM").alias("enrolled_on_ym"),
    )
    .agg(
        F.percentile_approx("gpa_filled", 0.5).alias("median_gpa_filled"),
        F.avg("gpa_filled").alias("avg_gpa_filled_check"),
    )
    .filter(F.col("avg_gpa_filled_check") > 1.9)
    .drop("avg_gpa_filled_check")
)

qualified = (
    summary
    .crossJoin(summary.agg(F.avg("median_gpa_filled").alias("overall_avg_median_gpa_filled")))
    .filter(F.col("median_gpa_filled") > F.col("overall_avg_median_gpa_filled"))
    .drop("overall_avg_median_gpa_filled")
)

labeled = (
    qualified
    .select(
        "is_international",
        "enrolled_on_ym",
        F.when(F.col("is_international"), 1).otherwise(0).alias("is_international_flag"),
        F.abs(F.col("median_gpa_filled") - 2.9).alias("median_gpa_filled_distance"),
    )
)

metrics = (
    labeled
    .select(
        "is_international",
        "enrolled_on_ym",
        "is_international_flag",
        F.lead("is_international_flag", 1, 0).over(w_all_by_median_gpa_filled_dis).alias("next_is_international_flag"),
        F.round(F.col("is_international_flag") / F.nullif(F.sum("is_international_flag").over(w_is_international), F.lit(0)), 4).alias("is_international_flag_share_of_is_international"),
        F.lag("is_international_flag").over(w_all_by_median_gpa_filled_dis).alias("prev_is_international_flag"),
        F.sum("is_international_flag").over(w_is_international).alias("is_international_sum_is_international_flag"),
        F.rank().over(w_all_by_median_gpa_filled_dis_2).alias("median_gpa_filled_distance_rank"),
        F.rank().over(w_all_by_median_gpa_filled_dis).alias("median_gpa_filled_distance_rank_2"),
    )
)

result = (
    metrics
    .select(
        "is_international",
        "next_is_international_flag",
        "prev_is_international_flag",
        "is_international_sum_is_international_flag",
        "median_gpa_filled_distance_rank",
        "median_gpa_filled_distance_rank_2",
        F.substring(F.trim(F.col("enrolled_on_ym")), 1, 4).alias("enrolled_on_ym_prefix"),
        F.abs(F.col("is_international_flag") - 1).alias("is_international_flag_distance"),
    )
    .orderBy(
        F.col("prev_is_international_flag").asc_nulls_last(),
        F.col("is_international_sum_is_international_flag").asc(),
    )
)
