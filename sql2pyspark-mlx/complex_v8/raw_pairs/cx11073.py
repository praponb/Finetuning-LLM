from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_score = Window.orderBy("score", "id")
w_term_student_id = Window.partitionBy("term", "student_id")
w_grade_status = Window.partitionBy("grade_status")
w_grade_status_term = Window.partitionBy("grade_status", "term")
w_all_by_score_prior_6 = Window.orderBy(F.col("score").desc(), "id").rowsBetween(-6, -1)
w_course_id_by_enrolled_at_ful = Window.partitionBy("course_id").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_id_by_enrolled_at_tie = Window.partitionBy("course_id").orderBy("enrolled_at")
w_distinct_student_id_in_grade = Window.partitionBy("distinct_student_id_in_grade_status", "course_id").orderBy("enrolled_at_dense_rank", "id")
w_distinct_student_id_in_grade_2 = Window.partitionBy("distinct_student_id_in_grade_status", "course_id").orderBy("enrolled_at_dense_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_student_id_by_last_score_tie = Window.partitionBy("student_id").orderBy(F.col("last_score").desc())

recent = (
    df_enrollments
    .filter((F.col("score").between(8.7, 59.7)) & (F.col("term").isNotNull()))
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

metrics = (
    recent
    .select(
        "*",
        F.row_number().over(w_all_by_score).alias("seq_no"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_term_student_id), F.lit(0)), 4).alias("score_share_of_term_student_id"),
        F.count(F.lit(1)).over(w_term_student_id).alias("rows_in_term_student_id"),
        F.count(F.lit(1)).over(w_grade_status).alias("rows_in_grade_status"),
        F.min("score").over(w_grade_status_term).alias("grade_status_term_min_score"),
        F.sum("score").over(w_grade_status).alias("grade_status_sum_score"),
        F.round(F.avg("score").over(w_grade_status), 2).alias("grade_status_avg_score"),
        F.dense_rank().over(w_all_by_score).alias("score_dense_rank"),
        F.round(F.avg("score").over(w_all_by_score_prior_6), 2).alias("score_avg_6_prior"),
        F.max("score").over(w_grade_status_term).alias("grade_status_term_max_score"),
        F.size(F.collect_set("student_id").over(w_grade_status)).alias("distinct_student_id_in_grade_status"),
        F.last("score").over(w_course_id_by_enrolled_at_ful).alias("last_score"),
        F.dense_rank().over(w_course_id_by_enrolled_at_tie).alias("enrolled_at_dense_rank"),
    )
)

with_windows = (
    metrics
    .select(
        "id",
        "term",
        "score",
        "grade_status",
        "score_share_of_term_student_id",
        "rows_in_term_student_id",
        "rows_in_grade_status",
        "grade_status_term_min_score",
        "grade_status_sum_score",
        "grade_status_avg_score",
        "score_dense_rank",
        "score_avg_6_prior",
        "grade_status_term_max_score",
        "distinct_student_id_in_grade_status",
        "enrolled_at_dense_rank",
        F.lead("seq_no", 2).over(w_distinct_student_id_in_grade).alias("next2_seq_no"),
        F.sum("score_avg_6_prior").over(w_distinct_student_id_in_grade_2).alias("running_score_avg_6_prior"),
        F.dense_rank().over(w_student_id_by_last_score_tie).alias("last_score_dense_rank"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "score",
        "score_share_of_term_student_id",
        "rows_in_grade_status",
        "grade_status_term_min_score",
        "grade_status_sum_score",
        "score_dense_rank",
        "score_avg_6_prior",
        "grade_status_term_max_score",
        "distinct_student_id_in_grade_status",
        "next2_seq_no",
        F.concat(F.col("term"), F.lit("#"), F.col("next2_seq_no").cast("string")).alias("term_ref"),
    )
    .orderBy(
        F.col("rows_in_grade_status").asc(),
        F.col("score_dense_rank").desc(),
        "id",
    )
    .limit(20)
)
