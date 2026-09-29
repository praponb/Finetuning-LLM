from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_total_enrollment_scor = Window.orderBy("total_enrollment_score", "id")
w_all_by_total_enrollment_scor_2 = Window.orderBy("total_enrollment_score", "id").rowsBetween(-3, Window.currentRow)
w_term_term_len = Window.partitionBy("term", "term_len")
w_student_id_by_enrollment_gra = Window.partitionBy("student_id").orderBy("enrollment_grade_status_count", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned = (
    df_enrollments
    .filter(F.col("course_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.length(F.trim(F.col("term"))).alias("term_len"),
        F.round(F.col("score") * 1.5, 2).alias("score_adj"),
        (F.col("enrolled_at") + F.expr("INTERVAL 30 MINUTES")).alias("enrolled_at_plus_30m"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.count("*").alias("enrollment_count"),
        F.sum("score").alias("total_enrollment_score"),
        F.min("score").alias("min_enrollment_score"),
    )
)

with_counts = (
    cleaned.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
        "e.total_enrollment_score",
        "e.min_enrollment_score",
    )
)

enrollment_stats_2 = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.max("enrolled_at").alias("last_enrollment_enrolled_at"),
        F.count(F.when(F.col("grade_status").isin("FAIL", "WITHDRAWN"), 1)).alias("enrollment_grade_status_count"),
    )
)

summarized_rows = (
    with_counts.alias("s")
    .join(enrollment_stats_2.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        "e.last_enrollment_enrolled_at",
        F.coalesce(F.col("e.enrollment_grade_status_count"), F.lit(0)).alias("enrollment_grade_status_count"),
    )
)

enriched = (
    summarized_rows
    .filter(F.col("score") >= 38.3)
    .select(
        "*",
        F.abs(F.col("total_enrollment_score") - 300.0).alias("total_enrollment_score_distance"),
        F.when(F.col("grade_status").isin("INCOMPLETE", "FAIL"), "group_a").when(F.col("grade_status") == "PASS", "group_b").otherwise("other").alias("grade_status_group"),
    )
)

sequenced = (
    enriched
    .select(
        "*",
        F.ntile(4).over(w_all_by_total_enrollment_scor).alias("total_enrollment_score_quartile"),
        F.round(F.avg("score").over(w_all_by_total_enrollment_scor_2), 2).alias("score_avg_4"),
        F.max("enrollment_count").over(w_all_by_total_enrollment_scor_2).alias("enrollment_count_max_4"),
        F.min("min_enrollment_score").over(w_term_term_len).alias("term_term_len_min_min_enrollment_score"),
        F.nth_value("enrolled_at_plus_30m", 2).over(w_student_id_by_enrollment_gra).alias("second_enrolled_at_plus_30m"),
    )
)

with_activity = (
    sequenced.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.enrolled_at") < F.lit("2025-02-12 12:00:00").cast("timestamp")), "left_anti")
)

result = (
    with_activity
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        "term_len",
        "score_adj",
        "enrolled_at_plus_30m",
        "enrollment_count",
        "total_enrollment_score",
        "min_enrollment_score",
        "enrollment_grade_status_count",
        "total_enrollment_score_distance",
        "grade_status_group",
        "term_term_len_min_min_enrollment_score",
        "second_enrolled_at_plus_30m",
        F.substring(F.trim(F.col("grade_status")), 1, 4).alias("grade_status_prefix"),
    )
    .orderBy(F.col("course_id").desc_nulls_first())
)
