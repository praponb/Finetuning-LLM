from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id_grade_status = Window.partitionBy("student_id", "grade_status")
w_student_id_by_score_prior_1 = Window.partitionBy("student_id").orderBy("score", "id").rowsBetween(-1, -1)
w_grade_status_student_id_by_s = Window.partitionBy("grade_status", "student_id").orderBy(F.col("score").desc(), "id").rowsBetween(-6, Window.currentRow)

base = (
    df_enrollments
    .filter(F.col("term") == "2025T2")
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

with_trends = (
    base
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.round(F.avg("score").over(w_student_id_grade_status), 2).alias("student_id_grade_status_avg_score"),
        F.round(F.avg("score").over(w_student_id_by_score_prior_1), 2).alias("score_avg_1_prior"),
        F.round(F.avg("score").over(w_grade_status_student_id_by_s), 2).alias("score_avg_7"),
    )
)

tagged = (
    with_trends
    .select(
        "*",
        F.date_trunc("day", F.col("enrolled_at")).alias("enrolled_at_day"),
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
    )
)

result = (
    tagged
    .filter(F.to_date(F.col("enrolled_at_day")) < F.lit("2025-02-09").cast("date"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        "student_id_grade_status_avg_score",
        "score_avg_1_prior",
        "enrolled_at_day",
        "score_bucket",
        "grade_status_len",
    )
    .orderBy(F.col("score").desc())
)
