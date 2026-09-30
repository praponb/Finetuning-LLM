from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_score_distance_2_prio = Window.orderBy("score_distance_2", "id").rowsBetween(-3, -1)
w_course_id = Window.partitionBy("course_id")

base = (
    df_enrollments
    .filter(F.col("student_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.date_format(F.col("enrolled_at"), "MMM yyyy").alias("enrolled_at_label"),
        F.abs(F.col("score") - 38.5).alias("score_distance"),
        F.abs(F.col("score") - 57.8).alias("score_distance_2"),
    )
)

metrics = (
    base
    .select(
        "*",
        F.round(F.avg("score_distance").over(w_all_by_score_distance_2_prio), 2).alias("score_distance_avg_3_prior"),
        F.round(F.col("score_distance") - F.avg("score_distance").over(w_course_id), 2).alias("score_distance_vs_course_id_avg"),
        F.round(F.col("score_distance") / F.nullif(F.sum("score_distance").over(w_course_id), F.lit(0)), 4).alias("score_distance_share_of_course_id"),
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

with_stats = (
    metrics.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        "e.last_enrollment_enrolled_at",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

result = (
    with_stats
    .select(
        "id",
        "score_distance",
        "score_distance_2",
        "score_distance_avg_3_prior",
    )
    .unpivot(
        ids=["id"],
        values=["score_distance", "score_distance_2", "score_distance_avg_3_prior"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("id", "metric_name")
)
