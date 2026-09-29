from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("enrollment_course_id", "course_id").orderBy(F.col("enrollment_enrolled_at").desc(), F.col("id").desc(), F.col("enrollment_id").desc())

cleaned = (
    df_enrollments
    .filter((F.col("course_id").isNotNull()) & (F.col("enrolled_at") < F.lit("2025-02-18 08:00:00").cast("timestamp")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

detail_rows = (
    cleaned.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.course_id") <= 8), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.enrolled_at",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
        F.col("e.course_id").alias("enrollment_course_id"),
    )
)

with_details_course = (
    detail_rows.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.enrollment_id",
        "s.enrollment_grade_status",
        "s.enrollment_enrolled_at",
        "s.enrollment_course_id",
        F.col("c.level").alias("course_level"),
        "c.course_code",
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.count(F.when(F.col("term").isNotNull(), 1)).alias("enrollment_term_count"),
    )
)

with_stats = (
    with_details_course.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_term_count"), F.lit(0)).alias("enrollment_term_count"),
    )
)

unique_rows = (
    with_stats
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

qualified = (
    unique_rows.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.score") > 5.9) & (F.col("e.enrolled_at") < F.lit("2025-02-18 08:00:00").cast("timestamp")), "left_anti")
)

with_parent_student = (
    qualified.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .join(df_enrollments.alias("e"), F.col("e.id") == F.col("s.enrollment_id"), "left")
    .select(
        "s.*",
        F.col("st.is_international").alias("student_is_international"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at_2"),
    )
)

result = (
    with_parent_student
    .select(
        "id",
        "term",
        "enrollment_id",
        "enrollment_enrolled_at",
        "enrollment_course_id",
        "course_level",
        "course_code",
        "enrollment_term_count",
        "student_is_international",
        "enrollment_enrolled_at_2",
        F.round((F.unix_timestamp(F.col("enrollment_enrolled_at")) - F.unix_timestamp(F.col("enrollment_enrolled_at_2"))) / 3600, 1).alias("hours_enrollment_enrolled_at_2_to_enrollment_enr"),
    )
    .orderBy(F.col("enrollment_term_count").asc(), "id", "enrollment_id")
    .limit(10)
)
