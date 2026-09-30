from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_group = Window.partitionBy("grade_status_group")
w_all_by_stddev_score = Window.orderBy("stddev_score", "grade_status_group")

filtered_enrollments = (
    df_enrollments
    .filter(F.col("enrolled_at") < F.lit("2025-02-24 09:00:00").cast("timestamp"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
        F.when(F.col("grade_status").isin("FAIL", "PASS"), "group_a").when(F.col("grade_status") == "INCOMPLETE", "group_b").otherwise("other").alias("grade_status_group"),
    )
)

grouped = (
    filtered_enrollments
    .filter(F.col("grade_status_len") < 12)
    .groupBy("grade_status_group")
    .agg(
        F.round(F.stddev("score"), 2).alias("stddev_score"),
        F.round(F.sum("score") / F.nullif(F.sum("grade_status_len"), F.lit(0)), 3).alias("score_to_grade_status_len_ratio"),
        F.min("enrolled_at").alias("first_enrolled_at"),
        F.countDistinct("student_id").alias("distinct_student_ids"),
        F.percentile_approx("score", 0.5).alias("median_score"),
        F.count("*").alias("n_rows"),
    )
)

filtered = (
    grouped
    .crossJoin(grouped.agg(F.avg("median_score").alias("overall_avg_median_score")))
    .filter(F.col("median_score") > F.col("overall_avg_median_score"))
    .drop("overall_avg_median_score")
)

ranked = (
    filtered
    .select(
        "*",
        F.max("distinct_student_ids").over(w_grade_status_group).alias("grade_status_group_max_distinct_student_ids"),
        (F.col("distinct_student_ids") - F.lag("distinct_student_ids").over(w_all_by_stddev_score)).alias("distinct_student_ids_change"),
        F.max("median_score").over(w_grade_status_group).alias("grade_status_group_max_median_score"),
        F.min("median_score").over(w_grade_status_group).alias("grade_status_group_min_median_score"),
        F.round(F.col("n_rows") - F.avg("n_rows").over(w_grade_status_group), 2).alias("n_rows_vs_grade_status_group_avg"),
    )
)

result = (
    ranked
    .groupBy("distinct_student_ids")
    .pivot("grade_status_group", ["other", "group_a", "group_b"])
    .agg(F.sum("grade_status_group_max_median_score"))
    .orderBy("distinct_student_ids")
)
