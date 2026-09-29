from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_enrolled_on_full = Window.orderBy(F.col("enrolled_on").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_cohort_major_by_gpa = Window.partitionBy("cohort", "major").orderBy(F.col("gpa").desc(), "id")
w_all_by_enrolled_on = Window.orderBy(F.col("enrolled_on").desc(), "id")
w_cohort_major_by_gpa_running = Window.partitionBy("cohort", "major").orderBy(F.col("gpa").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_major_is_international = Window.partitionBy("major", "is_international")
w_is_international_enrolled_on = Window.partitionBy("is_international", F.year("enrolled_on"))
w_all_by_gpa_prior_3 = Window.orderBy("gpa", "id").rowsBetween(-3, -1)
w_major_enrolled_on_dense_rank = Window.partitionBy("major", "enrolled_on_dense_rank")
w_major_enrolled_on_dense_rank_2 = Window.partitionBy("major", "enrolled_on_dense_rank").orderBy(F.col("enrolled_on_dense_rank").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_major_is_international_by_en = Window.partitionBy("major", "is_international").orderBy(F.col("enrolled_on").desc(), "id")

filtered_students = (
    df_students
    .filter((F.col("major") == "Biz") & (F.upper(F.trim(F.col("student_name"))).like("H%")))
    .select(
        "id",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
    )
)

with_windows = (
    filtered_students
    .select(
        "*",
        F.last("gpa").over(w_all_by_enrolled_on_full).alias("last_gpa"),
        F.lag("enrolled_on", 2).over(w_cohort_major_by_gpa).alias("prev2_enrolled_on"),
        F.dense_rank().over(w_all_by_enrolled_on).alias("enrolled_on_dense_rank"),
        F.lead("gpa").over(w_all_by_enrolled_on).alias("next_gpa"),
        F.min("gpa").over(w_cohort_major_by_gpa_running).alias("min_gpa_so_far"),
        F.count(F.lit(1)).over(w_major_is_international).alias("rows_in_major_is_international"),
        F.lag("gpa").over(w_cohort_major_by_gpa).alias("prev_gpa"),
        F.round(F.col("gpa") - F.avg("gpa").over(w_is_international_enrolled_on), 2).alias("gpa_vs_is_international_enrolled_on_year_avg"),
        F.max("gpa").over(w_all_by_gpa_prior_3).alias("gpa_max_3_prior"),
    )
)

windowed = (
    with_windows
    .select(
        "*",
        F.count(F.lit(1)).over(w_major_enrolled_on_dense_rank).alias("rows_in_major_enrolled_on_dense_rank"),
        F.round(F.col("last_gpa") - F.avg("last_gpa").over(w_major_enrolled_on_dense_rank), 2).alias("last_gpa_vs_major_enrolled_on_dense_rank_avg"),
        F.min("next_gpa").over(w_major_enrolled_on_dense_rank).alias("major_enrolled_on_dense_rank_min_next_gpa"),
        F.first("min_gpa_so_far").over(w_major_enrolled_on_dense_rank_2).alias("first_min_gpa_so_far"),
        F.lead("enrolled_on_dense_rank", 1, 0).over(w_major_is_international_by_en).alias("next_enrolled_on_dense_rank"),
        F.lag("gpa_max_3_prior").over(w_major_is_international_by_en).alias("prev_gpa_max_3_prior"),
    )
)

totals = (
    windowed
    .groupBy(
        "enrolled_on_dense_rank",
        F.date_trunc("month", "prev2_enrolled_on").alias("prev2_enrolled_on_month"),
        F.date_trunc("month", "enrolled_on").alias("enrolled_on_month"),
    )
    .agg(
        F.count("*").alias("n_rows"),
        F.countDistinct("id").alias("distinct_ids"),
        F.avg("rows_in_major_enrolled_on_dense_rank").alias("avg_rows_in_major_enrolled_on_dense_rank_check"),
    )
    .filter(F.col("avg_rows_in_major_enrolled_on_dense_rank_check") > 8)
    .drop("avg_rows_in_major_enrolled_on_dense_rank_check")
)

result = (
    totals
    .filter(F.col("prev2_enrolled_on_month") >= F.lit("2025-01-17 09:00:00").cast("timestamp"))
    .select(
        "enrolled_on_dense_rank",
        "prev2_enrolled_on_month",
        "enrolled_on_month",
        F.date_format(F.col("enrolled_on_month"), "yyyy-MM-dd").alias("enrolled_on_month_label"),
        (F.col("distinct_ids") - F.col("enrolled_on_dense_rank")).alias("distinct_ids_minus_enrolled_on_dense_rank"),
    )
    .orderBy("enrolled_on_dense_rank")
)
