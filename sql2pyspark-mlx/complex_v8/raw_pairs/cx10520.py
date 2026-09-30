from pyspark.sql import functions as F

filtered = (
    df_courses
    .filter(F.col("dept").isin("CS", "Art"))
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
        F.abs(F.col("credits") - 2).alias("credits_distance"),
    )
)

with_children_enrollments = (
    filtered.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.id")) & (F.col("e.term") == "2025T1"), "left")
    .select(
        "s.id",
        "s.dept",
        "s.credits",
        "s.level",
        "s.credits_distance",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
    )
)

qualified = (
    with_children_enrollments.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.id")) & (F.col("e.grade_status") == "WITHDRAWN"), "left_semi")
)

result = (
    qualified
    .select(
        "id",
        "dept",
        "credits",
        "level",
        "credits_distance",
        "enrollment_id",
        "enrollment_term",
        "enrollment_enrolled_at",
        F.date_format(F.col("enrollment_enrolled_at"), "yyyy-MM-dd").alias("enrollment_enrolled_at_label"),
    )
    .orderBy(F.col("enrollment_term").desc_nulls_first(), "id", "enrollment_id")
    .limit(5)
)
