from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id = Window.partitionBy("course_id")
w_all_by_enrollment_count_ties = Window.orderBy("enrollment_count")
w_all_by_enrollment_count = Window.orderBy("enrollment_count", "id")

prepared_enrollments = (
    df_enrollments
    .filter(F.col("student_id") <= 8)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.substring(F.trim(F.col("grade_status")), 1, 3).alias("grade_status_prefix"),
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
        F.when(F.col("grade_status").isin("WITHDRAWN", "PASS"), "group_a").when(F.col("grade_status") == "FAIL", "group_b").otherwise("other").alias("grade_status_group"),
    )
)

tagged = (
    prepared_enrollments
    .filter(F.col("grade_status") != "FAIL")
    .select(
        "*",
        F.upper(F.trim(F.col("grade_status_group"))).alias("grade_status_group_clean"),
        (F.floor(F.col("score_filled") / 25) * 25).alias("score_filled_bucket"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.count("*").alias("enrollment_count"),
        F.min("enrolled_at").alias("first_enrollment_enrolled_at"),
        F.max("enrolled_at").alias("last_enrollment_enrolled_at"),
    )
)

summarized_rows = (
    tagged.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
        "e.first_enrollment_enrolled_at",
        "e.last_enrollment_enrolled_at",
    )
)

qualified = (
    summarized_rows.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.student_id").isNotNull()), "left_anti")
    .filter(F.col("student_id").isNotNull())
)

ranked = (
    qualified
    .select(
        "id",
        "enrolled_at",
        "grade_status_prefix",
        "score_filled",
        "grade_status_group",
        "enrollment_count",
        F.round(F.avg("score_filled_bucket").over(w_course_id), 2).alias("course_id_avg_score_filled_bucket"),
        F.round(F.col("score_filled_bucket") / F.nullif(F.sum("score_filled_bucket").over(w_course_id), F.lit(0)), 4).alias("score_filled_bucket_share_of_course_id"),
        F.round(F.cume_dist().over(w_all_by_enrollment_count_ties), 3).alias("enrollment_count_cume_dist"),
        F.lag("score_filled", 1, 0).over(w_all_by_enrollment_count).alias("prev_score_filled"),
        F.size(F.collect_set("grade_status_group").over(w_course_id)).alias("distinct_grade_status_group_in_course_id"),
    )
)

result = (
    ranked
    .select(
        "id",
        "enrolled_at",
        "grade_status_prefix",
        "score_filled",
        "grade_status_group",
        "course_id_avg_score_filled_bucket",
        "score_filled_bucket_share_of_course_id",
        "enrollment_count_cume_dist",
        "prev_score_filled",
    )
)
