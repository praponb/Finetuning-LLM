from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_score_tier_by_enrolled_at = Window.partitionBy("score_tier").orderBy("enrolled_at", "id", "enrollment_id")
w_score_tier_course_id = Window.partitionBy("score_tier", "course_id")
w_score_tier_by_score_bucket = Window.partitionBy("score_tier").orderBy("score_bucket", "id", "enrollment_id")

recent = (
    df_enrollments
    .filter((F.col("student_id") <= 6) & (F.col("term").isin("2025T2", "2024T1")))
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
        F.hour(F.col("enrolled_at")).alias("enrolled_at_hour"),
        F.when(F.col("score") >= 71.0, "HIGH").when(F.col("score") >= 42.4, "MEDIUM").otherwise("LOW").alias("score_tier"),
    )
)

detail_rows = (
    recent.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.enrolled_at",
        "s.score_bucket",
        "s.enrolled_at_hour",
        "s.score_tier",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.student_id").alias("enrollment_student_id"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.term").alias("enrollment_term"),
    )
)

with_windows = (
    detail_rows
    .select(
        "*",
        F.row_number().over(w_score_tier_by_enrolled_at).alias("enrolled_at_seq"),
        F.size(F.collect_set("enrollment_id").over(w_score_tier_course_id)).alias("distinct_enrollment_id_in_score_tier_course_id"),
        (F.col("score_bucket") - F.lag("score_bucket").over(w_score_tier_by_score_bucket)).alias("score_bucket_change"),
        F.round(F.cume_dist().over(w_score_tier_by_enrolled_at), 3).alias("enrolled_at_cume_dist"),
        F.round(F.col("score_bucket") / F.nullif(F.sum("score_bucket").over(w_score_tier_course_id), F.lit(0)), 4).alias("score_bucket_share_of_score_tier_course_id"),
    )
)

result = (
    with_windows
    .groupBy("score_tier", "enrolled_at")
    .pivot("enrollment_grade_status", ["WITHDRAWN", "INCOMPLETE", "PASS", "FAIL"])
    .agg(F.max("score_bucket_change"))
)
