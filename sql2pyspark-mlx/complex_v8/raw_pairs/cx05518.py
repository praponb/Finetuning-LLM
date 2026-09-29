from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id_by_enrolled_at_run = Window.partitionBy("course_id").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_term = Window.partitionBy("term")
w_course_id_by_enrolled_at = Window.partitionBy("course_id").orderBy(F.col("enrolled_at").desc(), "id")
w_all_by_enrolled_at = Window.orderBy(F.col("enrolled_at").desc(), "id")
w_course_id_by_score = Window.partitionBy("course_id").orderBy("score", "id")
w_course_id_by_score_moving_6 = Window.partitionBy("course_id").orderBy("score", "id").rowsBetween(-6, Window.currentRow)
w_term_enrolled_at_month_by_sc = Window.partitionBy("term", F.month("enrolled_at")).orderBy(F.col("score").desc(), "id").rowsBetween(-1, Window.currentRow)
w_term_enrolled_at_month_by_sc_2 = Window.partitionBy("term", F.month("enrolled_at")).orderBy(F.col("score").desc(), "id")
w_all_by_enrolled_at_prior_2 = Window.orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(-2, -1)
w_all_by_enrolled_at_ties = Window.orderBy(F.col("enrolled_at").desc())

filtered = (
    df_enrollments
    .filter(F.col("score").between(34.2, 94.2))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat_ws(": ", F.col("grade_status"), F.col("term")).alias("grade_status_term_label"),
        F.concat(F.col("grade_status"), F.lit("#"), F.col("student_id").cast("string")).alias("grade_status_ref"),
    )
)

windowed = (
    filtered
    .select(
        "id",
        "student_id",
        "grade_status",
        "enrolled_at",
        "grade_status_term_label",
        "grade_status_ref",
        F.first("enrolled_at", ignorenulls=True).over(w_course_id_by_enrolled_at_run).alias("first_known_enrolled_at"),
        F.round(F.col("score") - F.avg("score").over(w_term), 2).alias("score_vs_term_avg"),
        F.row_number().over(w_course_id_by_enrolled_at).alias("seq_no"),
        F.ntile(10).over(w_all_by_enrolled_at).alias("enrolled_at_decile"),
        F.dense_rank().over(w_course_id_by_score).alias("score_dense_rank"),
        F.sum("score").over(w_course_id_by_score_moving_6).alias("score_sum_7"),
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_all_by_enrolled_at))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
        F.rank().over(w_course_id_by_enrolled_at).alias("enrolled_at_rank"),
        F.row_number().over(w_all_by_enrolled_at).alias("enrolled_at_seq"),
        F.round(F.avg("score").over(w_term_enrolled_at_month_by_sc), 2).alias("score_avg_2"),
        (F.col("score") - F.lag("score").over(w_term_enrolled_at_month_by_sc_2)).alias("score_change"),
        F.round(F.avg("score").over(w_all_by_enrolled_at_prior_2), 2).alias("score_avg_2_prior"),
        F.round(F.cume_dist().over(w_all_by_enrolled_at_ties), 3).alias("enrolled_at_cume_dist"),
    )
)

labeled = (
    windowed
    .withColumn("grade_status_term_label_clean", F.lower(F.trim(F.col("grade_status_term_label"))))
)

result = (
    labeled
    .select(
        "id",
        "student_id",
        "grade_status",
        "grade_status_term_label",
        "grade_status_ref",
        "first_known_enrolled_at",
        "score_vs_term_avg",
        "seq_no",
        "enrolled_at_decile",
        "score_dense_rank",
        "score_avg_2",
        "score_change",
        "score_avg_2_prior",
        "grade_status_term_label_clean",
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
    )
)
