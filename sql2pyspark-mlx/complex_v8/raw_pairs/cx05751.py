from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_enrollment_id_course_dept_by = Window.partitionBy("enrollment_id", "course_dept").orderBy("days_since_enrolled_at", "id", "enrollment_id")
w_enrollment_id_course_dept_by_2 = Window.partitionBy("enrollment_id", "course_dept").orderBy("days_since_enrolled_at", "id", "enrollment_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped = (
    df_enrollments
    .filter(F.hour(F.col("enrolled_at")).between(8, 13))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat_ws("|", F.col("grade_status"), F.col("term")).alias("grade_status_term_label"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
    )
)

with_children = (
    scoped.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.enrolled_at") >= F.lit("2025-01-13 11:00:00").cast("timestamp")), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.days_since_enrolled_at",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.student_id").alias("enrollment_student_id"),
    )
)

enriched = (
    with_children
    .filter(F.col("enrollment_grade_status").isNotNull())
    .select(
        "id",
        "term",
        "score",
        "grade_status",
        "days_since_enrolled_at",
        "enrollment_id",
        "enrollment_score",
        F.date_trunc("week", F.col("enrollment_enrolled_at")).alias("enrollment_enrolled_at_week"),
        F.round(F.col("enrollment_score") * 1.5, 2).alias("enrollment_score_adj"),
        (F.floor(F.col("days_since_enrolled_at") / 100) * 100).alias("days_since_enrolled_at_bucket"),
    )
)

enriched_enrollment = (
    enriched.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.id") == F.col("s.enrollment_id"), "inner")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("e.course_id"), "left")
    .select(
        "s.id",
        "s.term",
        "s.grade_status",
        "s.days_since_enrolled_at",
        "s.enrollment_id",
        "s.enrollment_score",
        "s.enrollment_score_adj",
        "s.days_since_enrolled_at_bucket",
        F.col("e.student_id").alias("enrollment_student_id"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
        F.col("c.dept").alias("course_dept"),
        "c.course_code",
    )
)

windowed = (
    enriched_enrollment
    .select(
        "id",
        "term",
        "grade_status",
        "days_since_enrolled_at",
        "enrollment_id",
        "enrollment_score",
        "enrollment_score_adj",
        "days_since_enrolled_at_bucket",
        "enrollment_student_id",
        "enrollment_enrolled_at",
        "course_dept",
        "course_code",
        F.lag("days_since_enrolled_at").over(w_enrollment_id_course_dept_by).alias("prev_days_since_enrolled_at"),
        F.last("enrollment_enrolled_at").over(w_enrollment_id_course_dept_by_2).alias("last_enrollment_enrolled_at"),
    )
)

joined_enrollment = (
    windowed.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.id") == F.col("s.enrollment_id"), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("e.student_id"), "inner")
    .select(
        "s.id",
        "s.grade_status",
        "s.enrollment_id",
        "s.enrollment_score",
        "s.days_since_enrolled_at_bucket",
        "s.enrollment_student_id",
        "s.enrollment_enrolled_at",
        "s.course_dept",
        "s.course_code",
        "s.prev_days_since_enrolled_at",
        "s.last_enrollment_enrolled_at",
        F.col("e.course_id").alias("enrollment_course_id"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.student_id").alias("enrollment_student_id_2"),
        F.col("st.is_international").alias("student_is_international"),
        F.col("st.gpa").alias("student_gpa"),
    )
)

result = (
    joined_enrollment
    .select(
        "id",
        "grade_status",
        "enrollment_id",
        "enrollment_score",
        "days_since_enrolled_at_bucket",
        "enrollment_student_id",
        "enrollment_enrolled_at",
        "course_code",
        "prev_days_since_enrolled_at",
        "last_enrollment_enrolled_at",
        "enrollment_term",
        "student_is_international",
        "student_gpa",
        F.lower(F.regexp_replace(F.trim(F.col("grade_status")), " ", "_")).alias("grade_status_slug"),
        F.lower(F.regexp_replace(F.trim(F.col("course_code")), " ", "_")).alias("course_code_slug"),
    )
    .orderBy(F.col("enrollment_enrolled_at").desc(), F.col("student_gpa").desc())
)
