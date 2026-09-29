from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_enrolled_on_prior_1 = Window.orderBy("enrolled_on", "id").rowsBetween(-1, -1)
w_major_is_international = Window.partitionBy("major", "is_international")
w_enrolled_on_month = Window.partitionBy(F.month("enrolled_on"))
w_all_by_enrolled_on_running = Window.orderBy("enrolled_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_enrolled_on_ties = Window.orderBy("enrolled_on")
w_is_international_by_enrolled = Window.partitionBy("is_international").orderBy("enrolled_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_international_by_enrolled_2 = Window.partitionBy("is_international").orderBy("enrolled_on", "id")
w_major_cohort_by_gpa_running = Window.partitionBy("major", "cohort").orderBy(F.col("gpa").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_students = (
    df_students
    .filter((F.col("is_international")) & (F.col("cohort") == "C2021"))
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
    )
)

windowed = (
    filtered_students
    .select(
        "id",
        "major",
        "gpa",
        "is_international",
        F.sum("gpa").over(w_all_by_enrolled_on_prior_1).alias("gpa_sum_1_prior"),
        F.count(F.lit(1)).over(w_major_is_international).alias("rows_in_major_is_international"),
        F.round(F.col("gpa") / F.nullif(F.sum("gpa").over(w_enrolled_on_month), F.lit(0)), 4).alias("gpa_share_of_enrolled_on_month"),
        F.count(F.lit(1)).over(w_all_by_enrolled_on_running).alias("running_count"),
        F.dense_rank().over(w_all_by_enrolled_on_ties).alias("enrolled_on_dense_rank"),
        F.first("gpa").over(w_is_international_by_enrolled).alias("first_gpa"),
        F.ntile(10).over(w_is_international_by_enrolled_2).alias("enrolled_on_decile"),
        F.sum("gpa").over(w_major_cohort_by_gpa_running).alias("running_gpa"),
        F.max("gpa").over(w_enrolled_on_month).alias("enrolled_on_month_max_gpa"),
    )
)

totals = (
    windowed
    .groupBy("enrolled_on_dense_rank")
    .agg(
        F.countDistinct("major").alias("distinct_majors"),
        F.sum("rows_in_major_is_international").alias("total_rows_in_major_is_international"),
        F.round(F.sum("enrolled_on_month_max_gpa"), 2).alias("total_enrolled_on_month_max_gpa"),
        F.max("enrolled_on_decile").alias("max_enrolled_on_decile_check"),
    )
    .filter(F.col("max_enrolled_on_decile_check") > 3)
    .drop("max_enrolled_on_decile_check")
)

result = (
    totals
    .select(
        "enrolled_on_dense_rank",
        "distinct_majors",
        (F.floor(F.col("enrolled_on_dense_rank") / 2) * 2).alias("enrolled_on_dense_rank_bucket"),
        F.round(F.col("distinct_majors") / F.nullif(F.col("enrolled_on_dense_rank"), F.lit(0)), 3).alias("distinct_majors_per_enrolled_on_dense_rank"),
    )
)
