from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_student_gpa = Window.orderBy(F.col("student_gpa").asc_nulls_last(), "id")
w_course_id = Window.partitionBy("course_id")
w_all_by_student_gpa_prior_2 = Window.orderBy(F.col("student_gpa").asc_nulls_last(), "id").rowsBetween(-2, -1)
w_student_id_grade_status_by_s = Window.partitionBy("student_id", "grade_status").orderBy(F.col("student_gpa").desc(), "id")
w_student_id_grade_status_by_s_2 = Window.partitionBy("student_id", "grade_status").orderBy(F.col("student_gpa").desc(), "id").rowsBetween(-3, Window.currentRow)
w_all_by_student_gpa_full = Window.orderBy("student_gpa", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_score_distance_full = Window.orderBy("score_distance", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_student_gpa_ties = Window.orderBy(F.col("student_gpa").asc_nulls_last())
w_course_id_student_id_by_scor = Window.partitionBy("course_id", "student_id").orderBy(F.col("score_distance").desc())

base = (
    df_enrollments
    .filter((F.col("score") > 16.3) | (F.to_date(F.col("enrolled_at")) < F.lit("2025-03-14").cast("date")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.abs(F.col("score") - 63.9).alias("score_distance"),
        F.concat(F.col("term"), F.lit("#"), F.col("student_id").cast("string")).alias("term_ref"),
    )
)

with_details_student = (
    base.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        "s.score_distance",
        F.col("st.cohort").alias("student_cohort"),
        F.col("st.gpa").alias("student_gpa"),
        F.col("c.credits").alias("course_credits"),
        F.col("c.level").alias("course_level"),
    )
)

metrics = (
    with_details_student
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "score_distance",
        "student_gpa",
        "course_level",
        (F.col("course_credits") - F.lag("course_credits").over(w_all_by_student_gpa)).alias("course_credits_change"),
        F.count(F.lit(1)).over(w_course_id).alias("rows_in_course_id"),
        F.round(F.avg("score_distance").over(w_all_by_student_gpa_prior_2), 2).alias("score_distance_avg_2_prior"),
        F.round(F.percent_rank().over(w_student_id_grade_status_by_s), 3).alias("student_gpa_percent_rank"),
        F.sum("student_gpa").over(w_student_id_grade_status_by_s_2).alias("student_gpa_sum_4"),
        F.last("course_credits").over(w_all_by_student_gpa_full).alias("last_course_credits"),
        F.first("course_credits").over(w_all_by_score_distance_full).alias("first_course_credits"),
        F.round(F.cume_dist().over(w_all_by_student_gpa_ties), 3).alias("student_gpa_cume_dist"),
        F.round(F.percent_rank().over(w_course_id_student_id_by_scor), 3).alias("score_distance_percent_rank"),
    )
)

labeled = (
    metrics
    .select(
        "*",
        F.round(F.col("score") * 0.9, 2).alias("score_adj"),
        F.substring(F.trim(F.col("course_level")), 1, 3).alias("course_level_prefix"),
        (F.col("score") - F.col("student_gpa")).alias("score_minus_student_gpa"),
    )
)

result = (
    labeled
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "score_distance",
        "student_gpa",
        "course_level",
        "course_credits_change",
        "rows_in_course_id",
        "student_gpa_percent_rank",
        "last_course_credits",
        "first_course_credits",
        "score_distance_percent_rank",
        "score_adj",
        "course_level_prefix",
        "score_minus_student_gpa",
        F.round(F.col("student_gpa_sum_4") / F.nullif(F.col("score_distance"), F.lit(0)), 3).alias("student_gpa_sum_4_per_score_distance"),
    )
    .orderBy(
        F.col("rows_in_course_id").asc_nulls_last(),
        F.col("course_level").desc_nulls_first(),
    )
)
