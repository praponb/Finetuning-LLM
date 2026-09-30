from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("term").orderBy(F.col("enrolled_at").desc(), F.col("id").desc(), F.col("enrollment_id").desc())

scoped_enrollments = (
    df_enrollments
    .filter(F.col("student_id") >= 15)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.date_format(F.col("enrolled_at"), "yyyy-MM").alias("enrolled_at_label"),
    )
)

with_children = (
    scoped_enrollments.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.enrolled_at",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.student_id").alias("enrollment_student_id"),
    )
)

enriched = (
    with_children
    .filter(F.col("enrollment_id").isNotNull())
    .select(
        "*",
        F.date_format(F.col("enrolled_at"), "yyyy-MM-dd").alias("enrolled_at_label"),
        F.substring(F.trim(F.col("grade_status")), 1, 3).alias("grade_status_prefix"),
        F.lower(F.regexp_replace(F.trim(F.col("term")), " ", "_")).alias("term_slug"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.max("enrolled_at").alias("last_enrollment_enrolled_at"),
        F.count("*").alias("enrollment_count"),
        F.min("enrolled_at").alias("first_enrollment_enrolled_at"),
    )
)

with_counts = (
    enriched.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        "e.last_enrollment_enrolled_at",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
        "e.first_enrollment_enrolled_at",
    )
)

without_matches = (
    with_counts.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.grade_status").isin("PASS", "INCOMPLETE", "WITHDRAWN")), "left_anti")
    .filter(F.col("enrolled_at") >= F.lit("2025-02-06 11:00:00").cast("timestamp"))
)

unique_rows = (
    without_matches
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    unique_rows
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        "enrollment_id",
        "grade_status_prefix",
        "term_slug",
        "last_enrollment_enrolled_at",
        "enrollment_count",
        F.date_trunc("year", F.col("enrolled_at")).alias("enrolled_at_year"),
        F.coalesce(F.col("enrollment_count"), F.lit(0)).alias("enrollment_count_filled"),
    )
    .orderBy("term_slug", F.col("course_id").desc())
)
