from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("grade_status", "student_id").orderBy("grade_status_len", "id")
w_student_id_grade_status_by_g = Window.partitionBy("student_id", "grade_status").orderBy(F.col("grade_status_len").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_score = Window.orderBy("score", "id")
w_student_id_grade_status_len = Window.partitionBy("student_id", "grade_status_len")

scoped_enrollments = (
    df_enrollments
    .filter(F.col("course_id") <= 6)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
    )
)

with_activity = (
    scoped_enrollments.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.hour(F.col("e.enrolled_at")).between(10, 12)) & (F.col("e.score") < 76.8), "left_anti")
)

latest_per_key = (
    with_activity
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

with_trends = (
    latest_per_key
    .select(
        "*",
        F.nth_value("enrolled_at", 2).over(w_student_id_grade_status_by_g).alias("second_enrolled_at"),
        F.round(F.percent_rank().over(w_all_by_score), 3).alias("score_percent_rank"),
        F.round(F.col("grade_status_len") / F.nullif(F.sum("grade_status_len").over(w_student_id_grade_status_len), F.lit(0)), 4).alias("grade_status_len_share_of_student_id_grade_statu"),
        F.lead("score").over(w_all_by_score).alias("next_score"),
        F.min("score").over(w_student_id_grade_status_len).alias("student_id_grade_status_len_min_score"),
    )
)

result = (
    with_trends
    .groupBy("student_id", "second_enrolled_at")
    .pivot("grade_status", ["INCOMPLETE", "FAIL", "PASS", "WITHDRAWN"])
    .agg(F.min("score").alias("min_v"), F.count("score").alias("n"))
    .orderBy("student_id", "second_enrolled_at")
)
