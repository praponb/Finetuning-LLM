from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term_len_student_id_by_term = Window.partitionBy("term_len", "student_id").orderBy("term_len", "id")
w_grade_status = Window.partitionBy("grade_status")

recent_enrollments = (
    df_enrollments
    .filter(F.col("term").isin("2024T1", "2025T2", "2025T1"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.length(F.trim(F.col("term"))).alias("term_len"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.round(F.avg("score"), 2).alias("avg_enrollment_score"),
        F.count("*").alias("enrollment_count"),
    )
)

with_counts = (
    recent_enrollments.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        "e.avg_enrollment_score",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

with_trends = (
    with_counts
    .select(
        "*",
        F.ntile(4).over(w_term_len_student_id_by_term).alias("term_len_quartile"),
        F.round(F.percent_rank().over(w_term_len_student_id_by_term), 3).alias("term_len_percent_rank"),
        F.size(F.collect_set("student_id").over(w_grade_status)).alias("distinct_student_id_in_grade_status"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "score",
        "grade_status",
        "term_len",
        "avg_enrollment_score",
        "enrollment_count",
        "term_len_quartile",
        "term_len_percent_rank",
        "distinct_student_id_in_grade_status",
    )
    .orderBy(
        F.col("grade_status").asc_nulls_last(),
        F.col("term_len_percent_rank").desc(),
    )
)
