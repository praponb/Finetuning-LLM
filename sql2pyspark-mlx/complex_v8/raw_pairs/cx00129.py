from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("grade_status").orderBy(F.col("first_enrollment_enrolled_at").desc(), F.col("id").desc())
w_student_id = Window.partitionBy("student_id")
w_all_by_score_distance_moving = Window.orderBy(F.col("score_distance").desc(), "id").rowsBetween(-1, Window.currentRow)

prepared_enrollments = (
    df_enrollments
    .filter(F.col("course_id") >= 5)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
        F.abs(F.col("score") - 30.0).alias("score_distance"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.min("score").alias("min_enrollment_score"),
        F.count("*").alias("enrollment_count"),
    )
)

with_counts = (
    prepared_enrollments.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        "e.min_enrollment_score",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

enrollment_stats_2 = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.count(F.when(F.col("grade_status").isNotNull(), 1)).alias("enrollment_grade_status_count"),
        F.min("enrolled_at").alias("first_enrollment_enrolled_at"),
    )
)

profiled = (
    with_counts.alias("s")
    .join(enrollment_stats_2.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_grade_status_count"), F.lit(0)).alias("enrollment_grade_status_count"),
        "e.first_enrollment_enrolled_at",
    )
)

latest = (
    profiled
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

with_trends = (
    latest
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "enrolled_at",
        "score_bucket",
        "score_distance",
        "enrollment_count",
        "enrollment_grade_status_count",
        "first_enrollment_enrolled_at",
        F.count(F.lit(1)).over(w_student_id).alias("rows_in_student_id"),
        F.round(F.avg("enrollment_count").over(w_student_id), 2).alias("student_id_avg_enrollment_count"),
        F.round(F.avg("enrollment_count").over(w_all_by_score_distance_moving), 2).alias("enrollment_count_avg_2"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "student_id",
        "term",
        "score_distance",
        "enrollment_count",
        "enrollment_grade_status_count",
        "rows_in_student_id",
        "student_id_avg_enrollment_count",
        "enrollment_count_avg_2",
        F.coalesce(F.col("enrollment_count_avg_2"), F.lit(0.0)).alias("enrollment_count_avg_2_filled"),
        F.length(F.trim(F.col("term"))).alias("term_len"),
    )
    .orderBy(F.col("enrollment_count").asc_nulls_last())
)
