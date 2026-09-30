from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term_grade_status_by_course = Window.partitionBy("term", "grade_status").orderBy(F.col("course_credits").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_course_dept_by_course_credit = Window.partitionBy("course_dept").orderBy("course_credits")
w_term = Window.partitionBy("term")
w_course_credits_enrolled_at_m = Window.partitionBy("course_credits", F.date_trunc("month", "enrolled_at"))
w_course_dept_by_course_credit_2 = Window.partitionBy("course_dept").orderBy("course_credits", "id")
w_all_by_course_credits_full = Window.orderBy("course_credits", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_term_course_id = Window.partitionBy("term", "course_id")
w_course_dept_by_enrolled_at_r = Window.partitionBy("course_dept").orderBy(F.col("enrolled_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grade_status_by_course_credi = Window.partitionBy("grade_status").orderBy(F.col("course_credits").desc())
w_grade_status = Window.partitionBy("grade_status")
w_distinct_grade_status_in_ter = Window.partitionBy("distinct_grade_status_in_term_course_id").orderBy("distinct_grade_status_in_term_course_id", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_term_by_first_known_enrolled = Window.partitionBy("term").orderBy("first_known_enrolled_at", "id")
w_grade_status_by_course_credi_2 = Window.partitionBy("grade_status").orderBy(F.col("course_credits").desc(), "id").rowsBetween(-6, -1)

filtered_enrollments = (
    df_enrollments
    .filter(F.col("term") == "2025T1")
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
    )
)

combined_details = (
    filtered_enrollments.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .filter(F.col("c.dept") == "Math")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.enrolled_at",
        F.col("c.dept").alias("course_dept"),
        "c.course_code",
        F.col("c.credits").alias("course_credits"),
    )
)

with_windows = (
    combined_details
    .select(
        "*",
        F.first("enrolled_at", ignorenulls=True).over(w_term_grade_status_by_course).alias("first_known_enrolled_at"),
        F.rank().over(w_course_dept_by_course_credit).alias("course_credits_rank"),
        F.round(F.col("course_credits") - F.avg("course_credits").over(w_term), 2).alias("course_credits_vs_term_avg"),
        F.min("course_credits").over(w_course_credits_enrolled_at_m).alias("course_credits_enrolled_at_month_min_course_cred"),
        F.ntile(10).over(w_course_dept_by_course_credit_2).alias("course_credits_decile"),
        F.first("enrolled_at").over(w_all_by_course_credits_full).alias("first_enrolled_at"),
        F.size(F.collect_set("grade_status").over(w_term_course_id)).alias("distinct_grade_status_in_term_course_id"),
        F.sum("course_credits").over(w_course_dept_by_enrolled_at_r).alias("running_course_credits"),
    )
)

with_windows_2 = (
    with_windows
    .select(
        "id",
        "student_id",
        "term",
        "grade_status",
        "enrolled_at",
        "course_credits",
        "course_credits_rank",
        "course_credits_vs_term_avg",
        "course_credits_decile",
        "first_enrolled_at",
        "running_course_credits",
        F.dense_rank().over(w_grade_status_by_course_credi).alias("course_credits_dense_rank"),
        F.sum("running_course_credits").over(w_grade_status).alias("grade_status_sum_running_course_credits"),
        F.nth_value("course_credits_decile", 2).over(w_distinct_grade_status_in_ter).alias("second_course_credits_decile"),
        F.row_number().over(w_term_by_first_known_enrolled).alias("rn"),
        F.lag("course_credits_vs_term_avg", 2).over(w_term_by_first_known_enrolled).alias("prev2_course_credits_vs_term_avg"),
        F.sum("course_credits_decile").over(w_grade_status).alias("grade_status_sum_course_credits_decile"),
        F.round(F.avg("course_credits_decile").over(w_grade_status_by_course_credi_2), 2).alias("course_credits_decile_avg_6_prior"),
    )
)

result = (
    with_windows_2
    .select(
        "id",
        "student_id",
        "term",
        "grade_status",
        "enrolled_at",
        "course_credits",
        "course_credits_rank",
        "course_credits_decile",
        "first_enrolled_at",
        "running_course_credits",
        "course_credits_dense_rank",
        "grade_status_sum_running_course_credits",
        "second_course_credits_decile",
        "prev2_course_credits_vs_term_avg",
        "course_credits_decile_avg_6_prior",
        F.abs(F.col("course_credits_decile") - 6).alias("course_credits_decile_distance"),
    )
    .orderBy(F.col("grade_status_sum_running_course_credits").asc(), "id")
    .limit(5)
)
