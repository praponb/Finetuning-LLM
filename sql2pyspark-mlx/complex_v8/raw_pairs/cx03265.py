from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id = Window.partitionBy("course_id").orderBy("enrolled_at", "id")
w_course_id_running = Window.partitionBy("course_id").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_enrollments = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

with_prev = filtered_enrollments.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_course_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_enrolled_at").isNull() | (F.col("enrolled_at") > F.col("prev_enrolled_at") + F.expr("INTERVAL 60 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_course_id_running))

grouped = (
    sessionized
    .filter(F.col("enrolled_at") >= F.lit("2025-01-25 13:00:00").cast("timestamp"))
    .groupBy("new_session_flag", "course_id", "grade_status")
    .agg(
        F.round(F.stddev("session_id"), 2).alias("stddev_session_id"),
        F.round(F.sum("session_id") / F.nullif(F.sum("score"), F.lit(0)), 3).alias("session_id_to_score_ratio"),
        F.count(F.when(F.col("term") == "2025T2", 1)).alias("n_term_2025t2"),
        F.percentile_approx("new_session_flag", 0.5).alias("median_new_session_flag"),
        F.sum("session_id").alias("total_session_id"),
        F.round(F.sum("score") / F.nullif(F.sum("new_session_flag"), F.lit(0)), 3).alias("score_to_new_session_flag_ratio"),
    )
)

combined_details_course = (
    grouped.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.new_session_flag",
        "s.course_id",
        "s.grade_status",
        "s.stddev_session_id",
        "s.session_id_to_score_ratio",
        "s.median_new_session_flag",
        "s.total_session_id",
        "s.score_to_new_session_flag_ratio",
        "c.course_code",
    )
)

result = (
    combined_details_course
    .select(
        "new_session_flag",
        "course_id",
        "grade_status",
        "stddev_session_id",
        "median_new_session_flag",
        "score_to_new_session_flag_ratio",
        F.coalesce(F.col("stddev_session_id"), F.lit(0.0)).alias("stddev_session_id_filled"),
        F.substring(F.trim(F.col("course_code")), 1, 4).alias("course_code_prefix"),
    )
    .orderBy(
        F.col("stddev_session_id").desc_nulls_first(),
        "stddev_session_id_filled",
    )
)
