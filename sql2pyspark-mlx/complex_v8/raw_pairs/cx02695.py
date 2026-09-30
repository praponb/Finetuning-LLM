from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_score_ties = Window.orderBy(F.col("score").desc())
w_all_by_score_ties_2 = Window.orderBy("score")
w_all_by_score = Window.orderBy(F.col("score").desc(), "id")
w_course_id = Window.partitionBy("course_id")
w_all_by_score_2 = Window.orderBy("score", "id")
w_all_by_score_prior_3 = Window.orderBy(F.col("score").desc(), "id").rowsBetween(-3, -1)
w_course_id_by_score = Window.partitionBy("course_id").orderBy("score", "id")

recent_enrollments = (
    df_enrollments
    .filter((~F.col("grade_status").isin("PASS", "FAIL")) & (F.col("score") >= 34.2))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.date_format(F.col("enrolled_at"), "yyyy-MM").alias("enrolled_at_label"),
    )
)

with_windows = (
    recent_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        F.round(F.percent_rank().over(w_all_by_score_ties), 3).alias("score_percent_rank"),
        F.round(F.percent_rank().over(w_all_by_score_ties_2), 3).alias("score_percent_rank_2"),
        F.round(F.cume_dist().over(w_all_by_score), 3).alias("score_cume_dist"),
        F.round(F.col("score") - F.avg("score").over(w_course_id), 2).alias("score_vs_course_id_avg"),
        F.row_number().over(w_all_by_score_2).alias("seq_no"),
        F.sum("score").over(w_all_by_score_prior_3).alias("score_sum_3_prior"),
        (F.col("score") - F.lag("score").over(w_course_id_by_score)).alias("score_change"),
        F.row_number().over(w_course_id_by_score).alias("score_seq"),
    )
)

derived = (
    with_windows
    .filter(F.col("score_vs_course_id_avg") >= -50.0)
    .select(
        "*",
        F.least(F.col("score_cume_dist"), F.col("score_sum_3_prior")).alias("least_score_cume_dist_score_sum_3_prior"),
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
        F.initcap(F.trim(F.col("term"))).alias("term_clean"),
    )
)

result = (
    derived
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "score_percent_rank",
        "score_percent_rank_2",
        "score_cume_dist",
        "seq_no",
        "score_sum_3_prior",
        "score_change",
        "score_seq",
        "least_score_cume_dist_score_sum_3_prior",
        "term_clean",
        F.substring(F.trim(F.col("term_clean")), 1, 3).alias("term_clean_prefix"),
    )
    .orderBy("course_id", F.col("score_percent_rank_2").asc_nulls_last())
)
