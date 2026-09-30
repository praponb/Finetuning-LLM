from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_enrolled_at = Window.orderBy("enrolled_at", "id")
w_grade_status_by_enrolled_at = Window.partitionBy("grade_status").orderBy("enrolled_at", "id")
w_all_by_enrolled_at_ties = Window.orderBy("enrolled_at")
w_all_by_enrolled_at_full = Window.orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_grade_status_or_unknown = Window.partitionBy("grade_status_or_unknown")
w_term_by_score = Window.partitionBy("term").orderBy(F.col("score").asc_nulls_last(), "id")
w_student_id = Window.partitionBy("student_id")
w_term_by_score_running = Window.partitionBy("term").orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_enrolled_at_dense_rank_by_en = Window.partitionBy("enrolled_at_dense_rank").orderBy(F.col("enrolled_at_rank").desc_nulls_first(), "id").rowsBetween(-2, -1)
w_term_by_score_full = Window.partitionBy("term").orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid = (
    df_enrollments
    .filter(F.col("course_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
    )
)

ranked = (
    valid
    .select(
        "id",
        "student_id",
        "term",
        "score",
        "grade_status",
        F.round(F.percent_rank().over(w_all_by_enrolled_at), 3).alias("enrolled_at_percent_rank"),
        F.rank().over(w_grade_status_by_enrolled_at).alias("enrolled_at_rank"),
        F.lag("enrolled_at").over(w_grade_status_by_enrolled_at).alias("prev_enrolled_at"),
        F.rank().over(w_all_by_enrolled_at_ties).alias("enrolled_at_rank_2"),
        F.last("score").over(w_all_by_enrolled_at_full).alias("last_score"),
        F.round(F.percent_rank().over(w_grade_status_by_enrolled_at), 3).alias("enrolled_at_percent_rank_2"),
        F.round(F.col("score") - F.avg("score").over(w_grade_status_or_unknown), 2).alias("score_vs_grade_status_or_unknown_avg"),
        F.lag("enrolled_at").over(w_term_by_score).alias("prev_enrolled_at_2"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_student_id), F.lit(0)), 4).alias("score_share_of_student_id"),
        F.dense_rank().over(w_grade_status_by_enrolled_at).alias("enrolled_at_dense_rank"),
    )
)

top_rows = ranked.filter(F.col("enrolled_at_dense_rank") <= 5)

with_trends = (
    top_rows
    .select(
        "*",
        F.sum("score_share_of_student_id").over(w_term_by_score_running).alias("running_score_share_of_student_id"),
        F.round(F.avg("enrolled_at_percent_rank_2").over(w_enrolled_at_dense_rank_by_en), 2).alias("enrolled_at_percent_rank_2_avg_2_prior"),
        F.nth_value("prev_enrolled_at_2", 2).over(w_term_by_score_full).alias("second_prev_enrolled_at_2"),
    )
)

result = (
    with_trends
    .filter(F.col("grade_status") == "PASS")
    .select(
        "id",
        "student_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at_percent_rank",
        "enrolled_at_rank",
        "prev_enrolled_at",
        "enrolled_at_rank_2",
        "last_score",
        "score_vs_grade_status_or_unknown_avg",
        "prev_enrolled_at_2",
        "score_share_of_student_id",
        "enrolled_at_dense_rank",
        "running_score_share_of_student_id",
        "enrolled_at_percent_rank_2_avg_2_prior",
        F.year(F.col("prev_enrolled_at")).alias("prev_enrolled_at_year"),
    )
    .orderBy(
        F.col("enrolled_at_dense_rank").asc(),
        F.col("enrolled_at_rank").asc_nulls_last(),
        "id",
    )
    .limit(5)
)
