from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_term_by_score_p = Window.partitionBy("grade_status", "term").orderBy(F.col("score").asc_nulls_last(), "id").rowsBetween(-3, -1)
w_course_dept_student_cohort_b = Window.partitionBy("course_dept", "student_cohort").orderBy("score", "id")
w_course_id = Window.partitionBy("course_id")
w_course_dept_student_cohort_b_2 = Window.partitionBy("course_dept", "student_cohort").orderBy("score", "id").rowsBetween(-1, -1)
w_all_by_score = Window.orderBy("score", "id")
w_all_by_score_prior_2 = Window.orderBy("score", "id").rowsBetween(-2, -1)
w_grade_status_term_by_score_r = Window.partitionBy("grade_status", "term").orderBy(F.col("score").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_course_dept_student_cohort_b_3 = Window.partitionBy("course_dept", "student_cohort").orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grade_status_term_by_score = Window.partitionBy("grade_status", "term").orderBy(F.col("score").asc_nulls_last(), "id")

valid = df_enrollments.filter(F.col("student_id").isNotNull())

joined_course = (
    valid.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .filter(F.col("c.dept").isin("Bio", "Math"))
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        F.col("c.level").alias("course_level"),
        F.col("c.dept").alias("course_dept"),
        F.col("st.cohort").alias("student_cohort"),
        F.col("st.major").alias("student_major"),
    )
)

sequenced = (
    joined_course
    .select(
        "id",
        "student_id",
        "score",
        "grade_status",
        "course_level",
        F.sum("score").over(w_grade_status_term_by_score_p).alias("score_sum_3_prior"),
        F.ntile(3).over(w_course_dept_student_cohort_b).alias("score_tercile"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_course_id), F.lit(0)), 4).alias("score_share_of_course_id"),
        (F.col("score") - F.lag("score").over(w_course_dept_student_cohort_b)).alias("score_change"),
        F.sum("score").over(w_course_dept_student_cohort_b_2).alias("score_sum_1_prior"),
        F.round(F.col("score") - F.avg("score").over(w_course_id), 2).alias("score_vs_course_id_avg"),
        F.count(F.lit(1)).over(w_course_id).alias("rows_in_course_id"),
        F.round(F.percent_rank().over(w_course_dept_student_cohort_b), 3).alias("score_percent_rank"),
        F.lead("score").over(w_all_by_score).alias("next_score"),
        F.round(F.avg("score").over(w_all_by_score_prior_2), 2).alias("score_avg_2_prior"),
        F.min("score").over(w_grade_status_term_by_score_r).alias("min_score_so_far"),
        F.sum("score").over(w_course_dept_student_cohort_b_3).alias("running_score"),
        F.round(F.percent_rank().over(w_grade_status_term_by_score), 3).alias("score_percent_rank_2"),
    )
)

derived = (
    sequenced
    .select(
        "*",
        F.abs(F.col("score_vs_course_id_avg") - 20.0).alias("score_vs_course_id_avg_distance"),
    )
)

result = (
    derived
    .select(
        "id",
        "student_id",
        "score",
        "grade_status",
        "course_level",
        "score_sum_3_prior",
        "score_tercile",
        "score_share_of_course_id",
        "score_sum_1_prior",
        "rows_in_course_id",
        "score_percent_rank",
        "next_score",
        "min_score_so_far",
        "score_percent_rank_2",
        "score_vs_course_id_avg_distance",
        F.substring(F.trim(F.col("grade_status")), 1, 2).alias("grade_status_prefix"),
    )
    .orderBy(
        F.col("grade_status_prefix").desc(),
        F.col("score_share_of_course_id").desc(),
    )
)
