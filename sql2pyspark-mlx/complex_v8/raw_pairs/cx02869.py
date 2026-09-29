from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term_by_score_bucket_moving = Window.partitionBy("term").orderBy(F.col("score_bucket").desc(), "id").rowsBetween(-1, Window.currentRow)
w_score_bucket_tier = Window.partitionBy("score_bucket_tier")
w_grade_status_course_id = Window.partitionBy("grade_status", "course_id")
w_score_bucket_tier_by_score_b = Window.partitionBy("score_bucket_tier").orderBy(F.col("score_bucket_tier_avg_score_bucket").desc(), "id").rowsBetween(-6, Window.currentRow)
w_score_bucket_tier_by_score_b_2 = Window.partitionBy("score_bucket_tier").orderBy(F.col("score_bucket_tier_avg_score_bucket").desc())
w_term_by_score_avg_2 = Window.partitionBy("term").orderBy("score_avg_2", "id")
w_score_bucket_tier_by_score_b_3 = Window.partitionBy("score_bucket_tier").orderBy(F.col("score_bucket_tier_avg_score_bucket").desc(), "id")

scoped = (
    df_enrollments
    .filter((F.col("enrolled_at") >= F.lit("2025-01-19 15:00:00").cast("timestamp")) & (F.col("student_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.substring(F.trim(F.col("grade_status")), 1, 4).alias("grade_status_prefix"),
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
        F.concat_ws(": ", F.col("grade_status"), F.col("term")).alias("grade_status_term_label"),
    )
)

derived = (
    scoped
    .filter(F.col("grade_status_prefix").isNotNull())
    .select(
        "*",
        F.when(F.col("score_bucket") >= 80, "HIGH").when(F.col("score_bucket") >= 35, "MEDIUM").otherwise("LOW").alias("score_bucket_tier"),
    )
)

with_trends = (
    derived
    .withColumn("score_avg_2", F.round(F.avg("score").over(w_term_by_score_bucket_moving), 2))
    .withColumn("score_bucket_tier_avg_score_bucket", F.round(F.avg("score_bucket").over(w_score_bucket_tier), 2))
)

with_activity = (
    with_trends.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.student_id") <= 5), "left_semi")
    .filter(F.col("grade_status_term_label").isNotNull())
)

windowed = (
    with_activity
    .select(
        "*",
        F.round(F.col("score_bucket") / F.nullif(F.sum("score_bucket").over(w_grade_status_course_id), F.lit(0)), 4).alias("score_bucket_share_of_grade_status_course_id"),
        F.round(F.avg("score").over(w_score_bucket_tier_by_score_b), 2).alias("score_avg_7"),
        F.round(F.percent_rank().over(w_score_bucket_tier_by_score_b_2), 3).alias("score_bucket_tier_avg_score_bucket_percent_rank"),
        (F.col("score") - F.lag("score").over(w_term_by_score_avg_2)).alias("score_change"),
        F.lag("score_avg_2", 2).over(w_score_bucket_tier_by_score_b_3).alias("prev2_score_avg_2"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.count("*").alias("enrollment_count"),
        F.max("enrolled_at").alias("last_enrollment_enrolled_at"),
        F.count(F.when(F.col("grade_status") != "WITHDRAWN", 1)).alias("enrollment_grade_status_count"),
    )
)

profiled = (
    windowed.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
        "e.last_enrollment_enrolled_at",
        F.coalesce(F.col("e.enrollment_grade_status_count"), F.lit(0)).alias("enrollment_grade_status_count"),
    )
)

result = (
    profiled
    .filter(F.col("enrollment_grade_status_count") > 3)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "score_bucket",
        "grade_status_term_label",
        "score_bucket_tier",
        "score_avg_2",
        "score_bucket_share_of_grade_status_course_id",
        "score_change",
        "prev2_score_avg_2",
        "enrollment_count",
        "last_enrollment_enrolled_at",
        "enrollment_grade_status_count",
        F.concat_ws(" / ", F.col("grade_status_prefix"), F.col("score_bucket_tier")).alias("grade_status_prefix_score_bucket_tier_label"),
    )
    .orderBy(F.col("score_bucket").asc_nulls_last(), "grade_status_term_label")
)
