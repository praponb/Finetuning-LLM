from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id_by_enrolled_at = Window.partitionBy("student_id").orderBy("enrolled_at", "id")
w_enrolled_at_to_date = Window.partitionBy(F.to_date("enrolled_at"))
w_student_id_by_enrolled_at_fu = Window.partitionBy("student_id").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent_enrollments = (
    df_enrollments
    .filter(F.col("course_id") >= 4)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
        F.when(F.col("score") >= 69.6, "HIGH").when(F.col("score") >= 20.2, "MEDIUM").otherwise("LOW").alias("score_tier"),
    )
)

with_windows = (
    recent_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "score_bucket",
        F.lag("score").over(w_student_id_by_enrolled_at).alias("prev_score"),
        F.lag("enrolled_at", 2).over(w_student_id_by_enrolled_at).alias("prev2_enrolled_at"),
        F.size(F.collect_set("grade_status").over(w_enrolled_at_to_date)).alias("distinct_grade_status_in_enrolled_at_to_date"),
        F.lag("score_bucket").over(w_student_id_by_enrolled_at).alias("prev_score_bucket"),
        F.round(F.col("score_bucket") / F.nullif(F.sum("score_bucket").over(w_enrolled_at_to_date), F.lit(0)), 4).alias("score_bucket_share_of_enrolled_at_to_date"),
        F.last("score_bucket").over(w_student_id_by_enrolled_at_fu).alias("last_score_bucket"),
    )
)

labeled = (
    with_windows
    .filter(F.col("score") >= 6.2)
    .select(
        "id",
        "course_id",
        "term",
        "score_bucket",
        "prev_score",
        "prev2_enrolled_at",
        "distinct_grade_status_in_enrolled_at_to_date",
        "prev_score_bucket",
        "score_bucket_share_of_enrolled_at_to_date",
        F.round(F.col("prev_score") * 1.5, 2).alias("prev_score_adj"),
        F.when(F.col("score_bucket") >= 55, "A").when(F.col("score_bucket") >= 40, "B").otherwise("C").alias("score_bucket_tier"),
    )
)

result = (
    labeled
    .select(
        "id",
        "score_bucket_share_of_enrolled_at_to_date",
        "prev_score",
        "prev_score_adj",
    )
    .unpivot(
        ids=["id"],
        values=["score_bucket_share_of_enrolled_at_to_date", "prev_score", "prev_score_adj"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("id", "metric_name")
)
