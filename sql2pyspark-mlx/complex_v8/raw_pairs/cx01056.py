from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_score_ties = Window.orderBy("score")
w_term = Window.partitionBy("term")
w_course_dept_student_id_by_st = Window.partitionBy("course_dept", "student_id").orderBy("student_gpa", "id").rowsBetween(-3, -1)

cleaned = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
    )
)

enriched = (
    cleaned.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        F.col("c.dept").alias("course_dept"),
        "c.course_code",
        F.col("st.is_international").alias("student_is_international"),
        F.col("st.gpa").alias("student_gpa"),
    )
)

scored = (
    enriched
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "course_dept",
        "course_code",
        "student_gpa",
        F.concat(F.col("course_dept"), F.lit("#"), F.col("course_id").cast("string")).alias("course_dept_ref"),
    )
)

ranked = (
    scored
    .select(
        "id",
        "student_id",
        "term",
        "score",
        "course_code",
        "student_gpa",
        "course_dept_ref",
        F.rank().over(w_all_by_score_ties).alias("score_rank"),
        F.round(F.col("score") - F.avg("score").over(w_term), 2).alias("score_vs_term_avg"),
        F.max("student_gpa").over(w_course_dept_student_id_by_st).alias("student_gpa_max_3_prior"),
    )
)

filtered = (
    ranked.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.course_id").isNotNull()) & (F.hour(F.col("e.enrolled_at")).between(10, 12)), "left_anti")
)

with_activity = (
    filtered.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left_semi")
    .filter(~F.col("term").isin("2025T1", "2024T1"))
)

result = (
    with_activity
    .select(
        "id",
        "student_id",
        "term",
        "score",
        "student_gpa",
        "course_dept_ref",
        "score_rank",
        F.lower(F.regexp_replace(F.trim(F.col("course_dept_ref")), " ", "_")).alias("course_dept_ref_slug"),
        F.abs(F.col("score") - 59.7).alias("score_distance"),
    )
    .orderBy("course_dept_ref_slug", F.col("student_gpa").asc_nulls_last())
)
