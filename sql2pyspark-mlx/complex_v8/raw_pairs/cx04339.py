from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("course_id", "student_id").orderBy(F.col("enrolled_at_plus_30m").desc(), F.col("id").desc())
w_student_id_course_id_by_scor = Window.partitionBy("student_id", "course_id").orderBy("score")
w_all_by_score_running = Window.orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_student_id_course_id_by_scor_2 = Window.partitionBy("student_id", "course_id").orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_score = Window.orderBy("score", "id")
w_student_id_course_id = Window.partitionBy("student_id", "course_id")

scoped_enrollments = (
    df_enrollments
    .filter(F.col("term").isin("2025T2", "2024T2"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.concat(F.col("term"), F.lit("#"), F.col("student_id").cast("string")).alias("term_ref"),
        (F.col("enrolled_at") + F.expr("INTERVAL 30 MINUTES")).alias("enrolled_at_plus_30m"),
        F.concat_ws(": ", F.col("grade_status"), F.col("term")).alias("grade_status_term_label"),
    )
)

unique_rows = (
    scoped_enrollments
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_windows = (
    unique_rows
    .select(
        "*",
        F.dense_rank().over(w_student_id_course_id_by_scor).alias("score_dense_rank"),
        F.first("score", ignorenulls=True).over(w_all_by_score_running).alias("first_known_score"),
        F.first("enrolled_at_plus_30m", ignorenulls=True).over(w_student_id_course_id_by_scor_2).alias("first_known_enrolled_at_plus_30m"),
        F.lead("enrolled_at_plus_30m").over(w_all_by_score).alias("next_enrolled_at_plus_30m"),
        F.round(F.col("score") - F.avg("score").over(w_student_id_course_id), 2).alias("score_vs_student_id_course_id_avg"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "enrolled_at",
        "term_ref",
        "enrolled_at_plus_30m",
        "grade_status_term_label",
        "score_dense_rank",
        "first_known_score",
        "first_known_enrolled_at_plus_30m",
        "next_enrolled_at_plus_30m",
        "score_vs_student_id_course_id_avg",
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
        F.quarter(F.col("enrolled_at")).alias("enrolled_at_quarter"),
    )
    .orderBy(F.col("course_id").desc(), "enrolled_at")
)
