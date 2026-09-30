from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_by_score_full = Window.partitionBy("grade_status").orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_enrolled_at = Window.orderBy(F.col("enrolled_at").desc(), "id")

cleaned_enrollments = (
    df_enrollments
    .filter((F.col("student_id").isNotNull()) & (F.col("enrolled_at") < F.lit("2025-03-08 09:00:00").cast("timestamp")))
)

with_windows = (
    cleaned_enrollments
    .withColumn("second_enrolled_at", F.nth_value("enrolled_at", 2).over(w_grade_status_by_score_full))
    .withColumn("score_change", F.col("score") - F.lag("score").over(w_all_by_enrolled_at))
)

scored = (
    with_windows
    .select(
        "id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.substring(F.trim(F.col("grade_status")), 1, 4).alias("grade_status_prefix"),
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
    )
)

result = (
    scored
    .select(
        "id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "grade_status_prefix",
        "score_filled",
    )
    .orderBy(F.col("id").desc(), F.col("grade_status").desc())
)
