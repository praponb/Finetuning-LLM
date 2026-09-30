from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_by_term_len = Window.partitionBy("grade_status").orderBy("term_len", "id")
w_course_credits = Window.partitionBy("course_credits")
w_all_by_student_gpa_ties = Window.orderBy("student_gpa")
w_all_by_course_credits_change = Window.orderBy(F.col("course_credits_change").desc(), "id")
w_all_by_student_enrolled_on_t = Window.orderBy(F.col("student_enrolled_on").desc_nulls_first())

recent = (
    df_enrollments
    .filter(F.col("grade_status").isin("INCOMPLETE", "WITHDRAWN", "FAIL"))
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat_ws(": ", F.col("grade_status"), F.col("term")).alias("grade_status_term_label"),
        F.length(F.trim(F.col("term"))).alias("term_len"),
    )
)

joined_student = (
    recent.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status",
        "s.enrolled_at",
        "s.grade_status_term_label",
        "s.term_len",
        F.col("st.gpa").alias("student_gpa"),
        F.col("st.is_international").alias("student_is_international"),
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        F.col("c.dept").alias("course_dept"),
        F.col("c.credits").alias("course_credits"),
    )
)

with_trends = (
    joined_student
    .select(
        "*",
        (F.col("course_credits") - F.lag("course_credits").over(w_grade_status_by_term_len)).alias("course_credits_change"),
        F.max("course_credits").over(w_course_credits).alias("course_credits_max_course_credits"),
        F.dense_rank().over(w_all_by_student_gpa_ties).alias("student_gpa_dense_rank"),
    )
)

with_windows = (
    with_trends
    .select(
        "id",
        "student_id",
        "course_id",
        "enrolled_at",
        "grade_status_term_label",
        "term_len",
        "student_gpa",
        "student_is_international",
        "student_enrolled_on",
        "course_dept",
        "course_credits",
        "course_credits_change",
        "course_credits_max_course_credits",
        "student_gpa_dense_rank",
        F.rank().over(w_all_by_course_credits_change).alias("course_credits_change_rank"),
        F.rank().over(w_all_by_student_enrolled_on_t).alias("student_enrolled_on_rank"),
    )
)

enriched = (
    with_windows.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.enrolled_at",
        "s.grade_status_term_label",
        "s.term_len",
        "s.student_is_international",
        "s.student_enrolled_on",
        "s.course_dept",
        "s.course_credits",
        "s.course_credits_max_course_credits",
        "s.student_gpa_dense_rank",
        "s.course_credits_change_rank",
        "s.student_enrolled_on_rank",
        F.col("st.gpa").alias("student_gpa_2"),
        F.col("st.cohort").alias("student_cohort"),
    )
)

annotated = (
    enriched
    .select(
        "id",
        "student_id",
        "enrolled_at",
        "term_len",
        "student_is_international",
        "student_enrolled_on",
        "course_dept",
        "student_gpa_dense_rank",
        "course_credits_change_rank",
        "student_enrolled_on_rank",
        "student_gpa_2",
        "student_cohort",
        F.date_trunc("day", F.col("enrolled_at")).alias("enrolled_at_day"),
        F.lower(F.trim(F.col("course_dept"))).alias("course_dept_clean"),
    )
)

student_cohort_stats = (
    annotated
    .groupBy("student_cohort")
    .agg(F.avg("student_gpa_dense_rank").alias("avg_student_gpa_dense_rank"))
)

outliers = (
    annotated.alias("s")
    .join(student_cohort_stats.alias("g"), F.col("g.student_cohort") == F.col("s.student_cohort"), "inner")
    .filter(F.col("s.student_gpa_dense_rank") > F.col("g.avg_student_gpa_dense_rank"))
    .select(
        "s.*",
        "g.avg_student_gpa_dense_rank",
        F.round(F.col("s.student_gpa_dense_rank") - F.col("g.avg_student_gpa_dense_rank"), 2).alias("student_gpa_dense_rank_minus_avg"),
    )
)

result = (
    outliers
    .filter(F.col("course_credits_change_rank") > 2)
    .select(
        "id",
        "student_id",
        "term_len",
        "student_enrolled_on",
        "course_dept",
        "student_gpa_dense_rank",
        "course_credits_change_rank",
        "student_enrolled_on_rank",
        "student_cohort",
        "enrolled_at_day",
        "course_dept_clean",
        "avg_student_gpa_dense_rank",
        "student_gpa_dense_rank_minus_avg",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("student_enrolled_on")).alias("days_since_student_enrolled_on"),
        F.round(F.col("student_gpa_2") * 0.25, 2).alias("student_gpa_2_adj"),
    )
    .orderBy("course_dept")
)
