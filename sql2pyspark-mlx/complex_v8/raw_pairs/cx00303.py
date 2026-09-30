from pyspark.sql import functions as F

base_students = (
    df_students
    .filter(F.col("enrolled_on") >= F.lit("2024-10-09").cast("date"))
    .select(
        "id",
        "student_name",
        "major",
        "enrolled_on",
        "gpa",
        F.date_add(F.col("enrolled_on"), 365).alias("enrolled_on_plus_365d"),
        F.when(F.col("gpa") >= 3.2, "large").when(F.col("gpa") >= 2.1, "medium").otherwise("small").alias("gpa_tier"),
        F.round(F.col("gpa") * 1.07, 2).alias("gpa_adj"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.max("enrolled_at").alias("last_enrollment_enrolled_at"),
        F.count("*").alias("enrollment_count"),
    )
)

profiled = (
    base_students.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "e.last_enrollment_enrolled_at",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

detail_rows = (
    profiled.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.major",
        "s.gpa",
        "s.enrolled_on_plus_365d",
        "s.gpa_tier",
        "s.last_enrollment_enrolled_at",
        "s.enrollment_count",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.course_id").alias("enrollment_course_id"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.score").alias("enrollment_score"),
    )
)

result = (
    detail_rows
    .groupingSets(
        [["enrollment_count", "gpa_tier", "enrollment_grade_status"], ["enrollment_count"]],
        "enrollment_count", "gpa_tier", "enrollment_grade_status",
    )
    .agg(
        F.count(F.when(~F.col("major").isin("Art", "Bio"), 1)).alias("n_major_not_in_art_bio"),
        F.round(F.stddev("enrollment_count"), 2).alias("stddev_enrollment_count"),
        F.sum("enrollment_score").alias("total_enrollment_score"),
    )
    .select(
        "enrollment_count",
        F.coalesce(F.col("gpa_tier"), F.lit("ALL")).alias("gpa_tier"),
        F.coalesce(F.col("enrollment_grade_status"), F.lit("ALL")).alias("enrollment_grade_status"),
        "n_major_not_in_art_bio",
        "stddev_enrollment_count",
        "total_enrollment_score",
    )
    .orderBy(
        F.col("enrollment_count").asc_nulls_last(),
        F.col("gpa_tier").asc_nulls_last(),
        F.col("enrollment_grade_status").asc_nulls_last(),
    )
)
