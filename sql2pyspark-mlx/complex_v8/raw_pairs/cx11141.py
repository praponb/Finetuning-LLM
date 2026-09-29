from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term_enrollment_count_2 = Window.partitionBy("term", "enrollment_count_2")
w_enrollment_count_2_enrollmen = Window.partitionBy("enrollment_count_2", "enrollment_count").orderBy("score", "id")
w_latest = Window.partitionBy("student_id").orderBy("enrollment_enrolled_at", "id", "enrollment_id")

base_enrollments = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.count("*").alias("enrollment_count"),
        F.count("*").alias("enrollment_count_2"),
        F.sum("score").alias("total_enrollment_score"),
    )
)

with_counts = (
    base_enrollments.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
        F.coalesce(F.col("e.enrollment_count_2"), F.lit(0)).alias("enrollment_count_2"),
        "e.total_enrollment_score",
    )
)

with_trends = (
    with_counts
    .select(
        "*",
        F.size(F.collect_set("grade_status").over(w_term_enrollment_count_2)).alias("distinct_grade_status_in_term_enrollment_count_2"),
        F.round(F.col("total_enrollment_score") - F.avg("total_enrollment_score").over(w_term_enrollment_count_2), 2).alias("total_enrollment_score_vs_term_enrollment_count_"),
        F.ntile(5).over(w_enrollment_count_2_enrollmen).alias("score_quintile"),
    )
)

screened = (
    with_trends.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.term").isin("2025T2", "2025T1", "2024T2")) & (F.col("e.score") <= 78.8), "left_anti")
)

with_parent_student = (
    screened.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.days_since_enrolled_at",
        "s.enrollment_count",
        "s.total_enrollment_score",
        "s.total_enrollment_score_vs_term_enrollment_count_",
        "s.score_quintile",
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        F.col("c.level").alias("course_level"),
        "c.course_code",
    )
)

detail_rows_enrollments = (
    with_parent_student.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.total_enrollment_score",
        "s.score_quintile",
        "s.course_level",
        "s.course_code",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
        F.col("e.student_id").alias("enrollment_student_id"),
        F.col("e.term").alias("enrollment_term"),
    )
)

latest_per_key = (
    detail_rows_enrollments
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest_per_key
    .groupBy("enrollment_term", "enrollment_id")
    .pivot("grade_status", ["PASS", "FAIL", "INCOMPLETE"])
    .agg(F.min("total_enrollment_score"))
    .orderBy("enrollment_term", "enrollment_id")
)
