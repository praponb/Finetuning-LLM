from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_major_by_gpa_full = Window.partitionBy("major").orderBy("gpa", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_gpa = Window.orderBy("gpa", "id")
w_major_cohort = Window.partitionBy("major", "cohort")
w_all_by_gpa_full = Window.orderBy("gpa", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_cohort = Window.partitionBy("cohort")
w_cohort_major_by_gpa_full = Window.partitionBy("cohort", "major").orderBy(F.col("gpa").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_cohort_major_by_gpa_full_2 = Window.partitionBy("cohort", "major").orderBy("gpa", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_cohort_major_by_gpa = Window.partitionBy("cohort", "major").orderBy("gpa", "id")
w_major_by_gpa_running = Window.partitionBy("major").orderBy("gpa", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_gpa_prior_2 = Window.orderBy("gpa", "id").rowsBetween(-2, -1)
w_cohort_by_cohort_sum_gpa = Window.partitionBy("cohort").orderBy("cohort_sum_gpa", "id")
w_major = Window.partitionBy("major")

recent = (
    df_students
    .filter(F.col("gpa").between(2.5, 3.9))
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "gpa",
    )
)

sequenced = (
    recent
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        F.last("gpa").over(w_major_by_gpa_full).alias("last_gpa"),
        F.lead("gpa", 1, 0).over(w_all_by_gpa).alias("next_gpa"),
        F.round(F.col("gpa") - F.avg("gpa").over(w_major_cohort), 2).alias("gpa_vs_major_cohort_avg"),
        F.nth_value("gpa", 2).over(w_all_by_gpa_full).alias("second_gpa"),
        F.sum("gpa").over(w_cohort).alias("cohort_sum_gpa"),
        F.round(F.avg("gpa").over(w_major_cohort), 2).alias("major_cohort_avg_gpa"),
        F.first("gpa").over(w_all_by_gpa_full).alias("first_gpa"),
        F.nth_value("gpa", 2).over(w_cohort_major_by_gpa_full).alias("second_gpa_2"),
        F.nth_value("gpa", 2).over(w_cohort_major_by_gpa_full_2).alias("second_gpa_3"),
        (F.col("gpa") - F.lag("gpa").over(w_all_by_gpa)).alias("gpa_change"),
        F.max("gpa").over(w_major_cohort).alias("major_cohort_max_gpa"),
        F.round(F.percent_rank().over(w_cohort_major_by_gpa), 3).alias("gpa_percent_rank"),
        F.sum("gpa").over(w_major_by_gpa_running).alias("running_gpa"),
        F.sum("gpa").over(w_major_cohort).alias("major_cohort_sum_gpa"),
        F.max("gpa").over(w_all_by_gpa_prior_2).alias("gpa_max_2_prior"),
    )
)

metrics = (
    sequenced
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "last_gpa",
        "next_gpa",
        "second_gpa",
        "major_cohort_avg_gpa",
        "first_gpa",
        "second_gpa_2",
        "gpa_change",
        "major_cohort_max_gpa",
        "gpa_percent_rank",
        "major_cohort_sum_gpa",
        "gpa_max_2_prior",
        F.row_number().over(w_cohort_by_cohort_sum_gpa).alias("seq_no"),
        F.sum("gpa_percent_rank").over(w_major).alias("major_sum_gpa_percent_rank"),
        F.count(F.lit(1)).over(w_major_cohort).alias("rows_in_major_cohort"),
        F.round(F.col("last_gpa") - F.avg("last_gpa").over(w_major_cohort), 2).alias("last_gpa_vs_major_cohort_avg"),
        F.min("last_gpa").over(w_major).alias("major_min_last_gpa"),
    )
)

result = (
    metrics
    .filter(F.col("major_cohort_max_gpa") >= 15.0)
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "last_gpa",
        "next_gpa",
        "major_cohort_avg_gpa",
        "gpa_change",
        "major_cohort_max_gpa",
        "gpa_percent_rank",
        "major_cohort_sum_gpa",
        "gpa_max_2_prior",
        "rows_in_major_cohort",
        "last_gpa_vs_major_cohort_avg",
        "major_min_last_gpa",
        F.length(F.trim(F.col("major"))).alias("major_len"),
        F.greatest(F.col("second_gpa_2"), F.col("major_cohort_max_gpa")).alias("greatest_second_gpa_2_major_cohort_max_gpa"),
    )
    .orderBy(F.col("major_len").desc())
)
