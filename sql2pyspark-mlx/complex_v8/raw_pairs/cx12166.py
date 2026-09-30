from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_score_full = Window.orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_grade_status_by_score = Window.partitionBy("grade_status").orderBy("score", "id")
w_student_id = Window.partitionBy("student_id")
w_term_by_score = Window.partitionBy("term").orderBy(F.col("score").asc_nulls_last(), "id")
w_term_by_score_full = Window.partitionBy("term").orderBy(F.col("score").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered_enrollments = (
    df_enrollments
    .filter((F.col("course_id") >= 6) & (F.col("enrolled_at") < F.lit("2025-02-18 11:00:00").cast("timestamp")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.date_format(F.col("enrolled_at"), "yyyy-MM-dd HH:00").alias("enrolled_at_label"),
    )
)

windowed = (
    filtered_enrollments
    .select(
        "*",
        F.nth_value("score", 2).over(w_all_by_score_full).alias("second_score"),
        F.lead("score").over(w_grade_status_by_score).alias("next_score"),
        (F.col("score") - F.lag("score").over(w_grade_status_by_score)).alias("score_change"),
        F.round(F.avg("score").over(w_student_id), 2).alias("student_id_avg_score"),
        F.rank().over(w_term_by_score).alias("score_rank"),
        F.lag("score", 3).over(w_term_by_score).alias("prev3_score"),
        F.min("score").over(w_student_id).alias("student_id_min_score"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_student_id), F.lit(0)), 4).alias("score_share_of_student_id"),
        F.round(F.col("score") - F.avg("score").over(w_student_id), 2).alias("score_vs_student_id_avg"),
        (F.col("score") - F.lag("score").over(w_term_by_score)).alias("score_change_2"),
        F.count(F.lit(1)).over(w_student_id).alias("rows_in_student_id"),
        F.last("score").over(w_term_by_score_full).alias("last_score"),
    )
)

result = (
    windowed
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "second_score",
        "next_score",
        "score_rank",
        "prev3_score",
        "score_share_of_student_id",
        "score_vs_student_id_avg",
        "score_change_2",
        "last_score",
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
    )
    .orderBy("student_id", "id")
    .limit(5)
)
