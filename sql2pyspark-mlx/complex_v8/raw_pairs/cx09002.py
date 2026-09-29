from pyspark.sql import functions as F

prepared_courses = (
    df_courses
    .filter(F.col("dept") == "Art")
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
    )
)

exploded_detail_enrollments = (
    prepared_courses.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.dept",
        "s.credits",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
    )
)

annotated = (
    exploded_detail_enrollments
    .filter(F.col("dept") == "Math")
    .select(
        "*",
        F.when(F.col("credits") >= 4, "large").when(F.col("credits") >= 2, "medium").otherwise("small").alias("credits_tier"),
        F.length(F.trim(F.col("dept"))).alias("dept_len"),
    )
)

enriched = (
    annotated.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.id") == F.col("s.enrollment_id"), "left")
    .select(
        "s.id",
        "s.credits",
        "s.enrollment_id",
        "s.enrollment_enrolled_at",
        "s.enrollment_grade_status",
        "s.credits_tier",
        "s.dept_len",
        F.col("e.grade_status").alias("enrollment_grade_status_2"),
    )
)

joined = (
    enriched.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.id") == F.col("s.enrollment_id"), "inner")
    .filter(F.to_date(F.col("e.enrolled_at")) < F.lit("2025-02-24").cast("date"))
    .select(
        "s.*",
        F.col("e.student_id").alias("enrollment_student_id"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at_2"),
    )
)

tagged = (
    joined
    .select(
        "id",
        "credits",
        "enrollment_id",
        "enrollment_enrolled_at",
        "credits_tier",
        "dept_len",
        "enrollment_enrolled_at_2",
        F.round(F.col("dept_len") / F.nullif(F.col("credits"), F.lit(0)), 3).alias("dept_len_per_credits"),
        F.date_trunc("week", F.col("enrollment_enrolled_at_2")).alias("enrollment_enrolled_at_2_week"),
    )
)

result = (
    tagged
    .groupingSets(
        [["credits", "credits_tier", "enrollment_id"], ["credits"], []],
        "credits", "credits_tier", "enrollment_id",
    )
    .agg(
        F.percentile_approx("dept_len_per_credits", 0.5).alias("median_dept_len_per_credits"),
    )
    .orderBy(
        F.col("credits").asc_nulls_last(),
        F.col("credits_tier").asc_nulls_last(),
        F.col("enrollment_id").asc_nulls_last(),
    )
)
