from pyspark.sql import functions as F

cleaned = (
    df_courses
    .filter(F.col("course_code").isNotNull())
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
        F.coalesce(F.col("credits"), F.lit(0)).alias("credits_filled"),
        F.length(F.trim(F.col("dept"))).alias("dept_len"),
        F.concat(F.col("level"), F.lit("#"), F.col("id").cast("string")).alias("level_ref"),
    )
)

exploded_detail_enrollments = (
    cleaned.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.course_code",
        "s.level",
        "s.credits_filled",
        "s.dept_len",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
        F.col("e.student_id").alias("enrollment_student_id"),
    )
)

result = (
    exploded_detail_enrollments
    .select(
        "id",
        "enrollment_id",
        F.col("credits_filled").alias("credits_filled_v"),
        F.col("dept_len").alias("m_dept_len"),
    )
    .unpivot(
        ids=["id", "enrollment_id"],
        values=["credits_filled_v", "m_dept_len"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
