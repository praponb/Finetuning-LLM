from pyspark.sql import functions as F

prepared_courses = (
    df_courses
    .filter((F.length(F.trim(F.col("course_code"))) >= 6) | (F.col("level") == "intro"))
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
        F.concat_ws(": ", F.col("course_code"), F.col("dept")).alias("course_code_dept_label"),
        F.abs(F.col("credits") - 2).alias("credits_distance"),
        F.when(F.col("level").isin("intro", "advanced"), "group_a").when(F.col("level") == "core", "group_b").otherwise("other").alias("level_group"),
    )
)

exploded_detail_enrollments = (
    prepared_courses.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.level",
        "s.level_group",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.student_id").alias("enrollment_student_id"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
        F.col("e.score").alias("enrollment_score"),
    )
)

result = (
    exploded_detail_enrollments
    .rollup("enrollment_id", "enrollment_student_id", "level")
    .agg(F.max("enrollment_enrolled_at").alias("last_enrollment_enrolled_at"))
)
