from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term_enrolled_at_month = Window.partitionBy("term", F.date_trunc("month", "enrolled_at"))
w_student_id_by_score_full = Window.partitionBy("student_id").orderBy(F.col("score").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_term_student_id = Window.partitionBy("term", "student_id")
w_all_by_score_ties = Window.orderBy(F.col("score").desc())
w_all_by_enrolled_at_full = Window.orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_student_id_by_score = Window.partitionBy("student_id").orderBy(F.col("score").asc_nulls_last(), "id")
w_grade_status_student_id_by_e = Window.partitionBy("grade_status", "student_id").orderBy(F.col("enrolled_at").desc_nulls_first())

cleaned = df_enrollments.filter(F.col("term") != "2025T2")

with_trends = (
    cleaned
    .select(
        "*",
        F.max("score").over(w_term_enrolled_at_month).alias("term_enrolled_at_month_max_score"),
        F.first("enrolled_at").over(w_student_id_by_score_full).alias("first_enrolled_at"),
        F.round(F.col("score") - F.avg("score").over(w_term_student_id), 2).alias("score_vs_term_student_id_avg"),
        F.rank().over(w_all_by_score_ties).alias("score_rank"),
        F.nth_value("score", 2).over(w_all_by_enrolled_at_full).alias("second_score"),
        F.lag("score", 3).over(w_student_id_by_score).alias("prev3_score"),
        F.dense_rank().over(w_grade_status_student_id_by_e).alias("enrolled_at_dense_rank"),
        F.round(F.avg("score").over(w_term_student_id), 2).alias("term_student_id_avg_score"),
    )
)

result = (
    with_trends
    .filter(F.col("student_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        "term_enrolled_at_month_max_score",
        "first_enrolled_at",
        "score_rank",
        "second_score",
        "prev3_score",
        "enrolled_at_dense_rank",
    )
)
