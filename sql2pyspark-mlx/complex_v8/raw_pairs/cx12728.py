from pyspark.sql import functions as F

recent = (
    df_enrollments
    .filter(F.col("student_id") <= 17)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.length(F.trim(F.col("term"))).alias("term_len"),
        F.length(F.trim(F.col("term"))).alias("term_len_2"),
        F.length(F.trim(F.col("term"))).alias("term_len_3"),
    )
)

exploded_detail = (
    recent.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.enrolled_at",
        "s.term_len",
        "s.term_len_2",
        "s.term_len_3",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.student_id").alias("enrollment_student_id"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.count(F.when(F.col("term") == "2025T2", 1)).alias("enrollment_term_count"),
        F.min("enrolled_at").alias("first_enrollment_enrolled_at"),
    )
)

summarized_rows = (
    exploded_detail.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_term_count"), F.lit(0)).alias("enrollment_term_count"),
        "e.first_enrollment_enrolled_at",
    )
)

result = (
    summarized_rows
    .cube("term_len", "term")
    .agg(F.grouping_id().alias("grouping_level"), F.count("*").alias("cnt"))
)
