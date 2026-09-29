from pyspark.sql import functions as F

filtered_courses = (
    df_courses
    .filter(F.length(F.trim(F.col("course_code"))) >= 8)
    .select(
        "id",
        "course_code",
        "dept",
        "level",
        F.lower(F.regexp_replace(F.trim(F.col("level")), " ", "_")).alias("level_slug"),
    )
)

enriched = filtered_courses.withColumn("level_len", F.length(F.trim(F.col("level"))))

detail_rows = (
    enriched.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.id")) & (F.month(F.col("e.enrolled_at")).isin(1, 2)), "left")
    .select(
        "s.id",
        "s.course_code",
        "s.level",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.student_id").alias("enrollment_student_id"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
    )
)

result = (
    detail_rows
    .select(
        "id",
        "course_code",
        "level",
        "enrollment_id",
        "enrollment_term",
        "enrollment_score",
        "enrollment_enrolled_at",
    )
    .orderBy(F.col("enrollment_score").asc_nulls_last(), "id", "enrollment_id")
    .limit(20)
)
