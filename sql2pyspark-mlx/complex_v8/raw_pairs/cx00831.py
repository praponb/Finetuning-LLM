from pyspark.sql import functions as F

recent_enrollments = (
    df_enrollments
    .filter(F.col("student_id") >= 3)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.when(F.col("score") >= 74.9, "A").when(F.col("score") >= 32.9, "B").otherwise("C").alias("score_tier"),
    )
)

qualified = (
    recent_enrollments.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.term") == "2025T1"), "left_anti")
)

with_details = (
    qualified.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.score_tier",
        F.col("c.dept").alias("course_dept"),
        "c.course_code",
    )
)

totals = (
    with_details
    .groupBy("student_id", "term")
    .agg(
        F.count("*").alias("row_count"),
        F.sum(F.when(~F.col("course_dept").isin("Art", "Biz"), 1).otherwise(0)).alias("n_course_dept_not_in_art_biz"),
        F.countDistinct("score_tier").alias("distinct_score_tiers"),
    )
)

annotated = (
    totals
    .select(
        "*",
        F.concat(F.col("term"), F.lit("#"), F.col("distinct_score_tiers").cast("string")).alias("term_ref"),
        (F.col("distinct_score_tiers") - F.col("row_count")).alias("distinct_score_tiers_minus_row_count"),
    )
)

exploded_detail_enrollments = (
    annotated.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.student_id",
        "s.term",
        "s.n_course_dept_not_in_art_biz",
        "s.distinct_score_tiers",
        "s.term_ref",
        "s.distinct_score_tiers_minus_row_count",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.course_id").alias("enrollment_course_id"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
    )
)

combined_details_enrollment = (
    exploded_detail_enrollments.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.id") == F.col("s.enrollment_id"), "inner")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.enrollment_course_id"), "left")
    .filter(F.col("e.grade_status") == "FAIL")
    .select(
        "s.student_id",
        "s.term",
        "s.n_course_dept_not_in_art_biz",
        "s.distinct_score_tiers",
        "s.term_ref",
        "s.distinct_score_tiers_minus_row_count",
        "s.enrollment_id",
        "s.enrollment_course_id",
        "s.enrollment_score",
        "s.enrollment_grade_status",
        F.col("e.term").alias("enrollment_term"),
        F.col("e.grade_status").alias("enrollment_grade_status_2"),
        F.col("c.level").alias("course_level"),
        F.col("c.credits").alias("course_credits"),
    )
)

result = (
    combined_details_enrollment
    .select(
        "student_id",
        "term",
        "n_course_dept_not_in_art_biz",
        "term_ref",
        "distinct_score_tiers_minus_row_count",
        "enrollment_id",
        "enrollment_course_id",
        "enrollment_grade_status",
        "enrollment_term",
        "course_level",
        "course_credits",
    )
    .orderBy(F.col("enrollment_course_id").asc())
)
