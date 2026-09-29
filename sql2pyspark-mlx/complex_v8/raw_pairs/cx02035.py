from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term = Window.partitionBy("term")
w_term_by_score_ties = Window.partitionBy("term").orderBy(F.col("score").desc())
w_term_by_score = Window.partitionBy("term").orderBy(F.col("score").desc(), "id")

prepared_enrollments = (
    df_enrollments
    .filter((F.col("term") == "2025T2") & (F.col("enrolled_at") < F.lit("2025-03-08 11:00:00").cast("timestamp")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.initcap(F.trim(F.col("term"))).alias("term_clean"),
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
        F.upper(F.trim(F.col("term"))).alias("term_clean_2"),
    )
)

annotated = (
    prepared_enrollments
    .select(
        "id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        "term_clean",
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
        F.concat_ws(" / ", F.col("term_clean_2"), F.col("term")).alias("term_clean_2_term_label"),
        F.substring(F.trim(F.col("term_clean")), 1, 4).alias("term_clean_prefix"),
    )
)

with_windows = (
    annotated
    .select(
        "id",
        "course_id",
        "score",
        "enrolled_at",
        "term_clean",
        "grade_status_len",
        "term_clean_2_term_label",
        "term_clean_prefix",
        F.sum("grade_status_len").over(w_term).alias("term_sum_grade_status_len"),
        F.round(F.cume_dist().over(w_term_by_score_ties), 3).alias("score_cume_dist"),
        F.round(F.col("score") - F.avg("score").over(w_term), 2).alias("score_vs_term_avg"),
        F.min("score").over(w_term).alias("term_min_score"),
        F.row_number().over(w_term_by_score).alias("seq_no"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "course_id",
        "enrolled_at",
        "term_clean",
        "term_clean_2_term_label",
        "term_sum_grade_status_len",
        "score_cume_dist",
        "score_vs_term_avg",
        "term_min_score",
        "seq_no",
    )
    .orderBy(F.col("score_vs_term_avg").desc())
)
