from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("course_id", "student_id").orderBy(F.col("enrolled_at").desc(), F.col("id").desc())

cleaned = df_enrollments.filter(F.col("term") == "2025T2")

latest = (
    cleaned
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

joined = (
    latest.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .filter(F.upper(F.trim(F.col("c.course_code"))).like("C%"))
    .select(
        "s.*",
        F.col("c.level").alias("course_level"),
        "c.course_code",
        F.col("c.credits").alias("course_credits"),
        F.col("st.gpa").alias("student_gpa"),
        F.col("st.is_international").alias("student_is_international"),
    )
)

screened = (
    joined.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.hour(F.col("e.enrolled_at")).between(10, 12)) & (F.col("e.term") != "2025T2"), "left_anti")
)

line_level_enrollments = (
    screened.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.enrolled_at",
        "s.course_level",
        "s.course_code",
        "s.student_gpa",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.student_id").alias("enrollment_student_id"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
    )
)

with_parent_student = (
    line_level_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.enrolled_at",
        "s.course_level",
        "s.course_code",
        "s.student_gpa",
        "s.enrollment_id",
        "s.enrollment_term",
        "s.enrollment_student_id",
        "s.enrollment_score",
        "s.enrollment_grade_status",
        F.col("st.is_international").alias("student_is_international"),
        F.col("st.gpa").alias("student_gpa_2"),
        F.col("st.enrolled_on").alias("student_enrolled_on"),
    )
)

result = (
    with_parent_student
    .cube("enrollment_id", "student_is_international", "course_level")
    .agg(
        F.max("enrolled_at").alias("last_enrolled_at"),
        F.min("enrolled_at").alias("first_enrolled_at"),
    )
    .select(
        "enrollment_id",
        "student_is_international",
        F.coalesce(F.col("course_level"), F.lit("ALL")).alias("course_level"),
        "last_enrolled_at",
        "first_enrolled_at",
    )
    .orderBy(
        F.col("enrollment_id").asc_nulls_last(),
        F.col("student_is_international").asc_nulls_last(),
        F.col("course_level").asc_nulls_last(),
    )
)
