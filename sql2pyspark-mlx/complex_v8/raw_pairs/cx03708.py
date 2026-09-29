from pyspark.sql import functions as F

base = (
    df_courses
    .filter(F.lower(F.col("course_code")).like("%cs%"))
    .select(
        "id",
        "dept",
        "credits",
        "level",
        F.length(F.trim(F.col("level"))).alias("level_len"),
        F.concat_ws(" / ", F.col("level"), F.col("course_code")).alias("level_course_code_label"),
    )
)

exploded_detail = (
    base.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.id")) & (F.col("e.term") != "2024T2"), "inner")
    .select(
        "s.id",
        "s.level",
        "s.level_course_code_label",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
    )
)

result = (
    exploded_detail
    .select(
        "id",
        "level_course_code_label",
        "enrollment_id",
        "enrollment_term",
        "enrollment_grade_status",
        F.concat_ws("-", F.col("level"), F.col("enrollment_grade_status")).alias("level_enrollment_grade_status_label"),
    )
    .orderBy("level_enrollment_grade_status_label", F.col("id").desc())
)
