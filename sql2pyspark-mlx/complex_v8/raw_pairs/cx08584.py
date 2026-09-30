from pyspark.sql import functions as F

scoped = (
    df_courses
    .filter((F.upper(F.trim(F.col("course_code"))).like("C%")) & (F.col("credits") <= 3))
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
        F.when(F.col("level").isin("intro", "core"), "group_a").when(F.col("level") == "advanced", "group_b").otherwise("other").alias("level_group"),
        F.substring(F.trim(F.col("course_code")), 1, 2).alias("course_code_prefix"),
        F.abs(F.col("credits") - 2).alias("credits_distance"),
    )
)

exploded_detail_enrollments = (
    scoped.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.course_code",
        "s.dept",
        "s.credits",
        "s.level",
        "s.level_group",
        "s.course_code_prefix",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
    )
)

result = (
    exploded_detail_enrollments
    .cube("enrollment_grade_status", "level", "dept")
    .agg(
        F.grouping("enrollment_grade_status").alias("is_total_enrollment_grade_status"),
        F.min("enrollment_enrolled_at").alias("first_enrollment_enrolled_at"),
        F.min("credits").alias("min_credits"),
    )
    .orderBy(
        F.col("enrollment_grade_status").asc_nulls_last(),
        F.col("level").asc_nulls_last(),
        F.col("dept").asc_nulls_last(),
    )
)
