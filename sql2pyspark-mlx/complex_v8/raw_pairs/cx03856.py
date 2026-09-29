from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_dept_term_by_course_c = Window.partitionBy("course_dept", "term").orderBy("course_credits", "id")
w_course_dept_term_by_course_c_2 = Window.partitionBy("course_dept", "term").orderBy("course_credits", "id").rowsBetween(-6, -1)
w_course_dept_term_by_course_c_3 = Window.partitionBy("course_dept", "term").orderBy("course_credits")
w_course_credits_quintile_stud = Window.partitionBy("course_credits_quintile", "student_id").orderBy("course_credits", "id").rowsBetween(-1, -1)
w_course_credits_quintile_stud_2 = Window.partitionBy("course_credits_quintile", "student_id").orderBy("course_credits", "id").rowsBetween(-6, -1)
w_course_credits_max_6_prior_s = Window.partitionBy("course_credits_max_6_prior", "student_id")
w_all_by_course_credits_decile = Window.orderBy("course_credits_decile", "id")

valid = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.substring(F.trim(F.col("grade_status")), 1, 3).alias("grade_status_prefix"),
    )
)

enriched = (
    valid.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        F.col("c.credits").alias("course_credits"),
        F.col("c.dept").alias("course_dept"),
    )
)

screened = (
    enriched.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.score") > 43.5), "left_anti")
)

sequenced = (
    screened
    .select(
        "*",
        F.round(F.percent_rank().over(w_course_dept_term_by_course_c), 3).alias("course_credits_percent_rank"),
        F.ntile(5).over(w_course_dept_term_by_course_c).alias("course_credits_quintile"),
        F.ntile(10).over(w_course_dept_term_by_course_c).alias("course_credits_decile"),
        F.max("course_credits").over(w_course_dept_term_by_course_c_2).alias("course_credits_max_6_prior"),
        F.round(F.percent_rank().over(w_course_dept_term_by_course_c_3), 3).alias("course_credits_percent_rank_2"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(F.min("enrolled_at").alias("first_enrollment_enrolled_at"))
)

summarized_rows = (
    sequenced.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select("s.*", "e.first_enrollment_enrolled_at")
)

windowed = (
    summarized_rows
    .select(
        "id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "grade_status_prefix",
        "course_credits",
        "course_dept",
        "course_credits_decile",
        "course_credits_max_6_prior",
        "course_credits_percent_rank_2",
        "first_enrollment_enrolled_at",
        F.round(F.avg("course_credits_percent_rank").over(w_course_credits_quintile_stud), 2).alias("course_credits_percent_rank_avg_1_prior"),
        F.sum("course_credits_max_6_prior").over(w_course_credits_quintile_stud_2).alias("course_credits_max_6_prior_sum_6_prior"),
        F.count(F.lit(1)).over(w_course_credits_max_6_prior_s).alias("rows_in_course_credits_max_6_prior_student_id"),
        F.round(F.col("course_credits_percent_rank") - F.avg("course_credits_percent_rank").over(w_course_credits_max_6_prior_s), 2).alias("course_credits_percent_rank_vs_course_credits_ma"),
        F.round(F.cume_dist().over(w_all_by_course_credits_decile), 3).alias("course_credits_decile_cume_dist"),
    )
)

result = (
    windowed
    .select(
        "id",
        "course_id",
        "grade_status",
        "course_dept",
        "course_credits_decile",
        "course_credits_max_6_prior",
        "first_enrollment_enrolled_at",
        "course_credits_max_6_prior_sum_6_prior",
        "rows_in_course_credits_max_6_prior_student_id",
        "course_credits_percent_rank_vs_course_credits_ma",
        "course_credits_decile_cume_dist",
        F.lower(F.regexp_replace(F.trim(F.col("grade_status")), " ", "_")).alias("grade_status_slug"),
    )
)
