from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_score_filled_full = Window.orderBy("score_filled", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_student_id = Window.partitionBy("student_id")
w_student_id_score_tier_by_sco = Window.partitionBy("student_id", "score_tier").orderBy(F.col("score_filled").asc_nulls_last(), "id")

base_enrollments = (
    df_enrollments
    .filter(F.col("course_id") <= 2)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
        F.when(F.col("score") >= 66.0, "A").when(F.col("score") >= 38.5, "B").otherwise("C").alias("score_tier"),
    )
)

metrics = (
    base_enrollments
    .select(
        "id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        "score_filled",
        F.first("score_filled").over(w_all_by_score_filled_full).alias("first_score_filled"),
        F.max("score_filled").over(w_student_id).alias("student_id_max_score_filled"),
        F.round(F.percent_rank().over(w_student_id_score_tier_by_sco), 3).alias("score_filled_percent_rank"),
        F.round(F.col("score_filled") - F.avg("score_filled").over(w_student_id), 2).alias("score_filled_vs_student_id_avg"),
        F.size(F.collect_set("term").over(w_student_id)).alias("distinct_term_in_student_id"),
    )
)

result = (
    metrics
    .rollup("grade_status", "course_id")
    .agg(
        F.countDistinct("course_id").alias("distinct_course_ids"),
        F.sum(F.when(F.col("grade_status") == "PASS", F.col("first_score_filled"))).alias("first_score_filled_grade_status_pass"),
    )
    .orderBy(
        F.col("grade_status").asc_nulls_last(),
        F.col("course_id").asc_nulls_last(),
    )
)
