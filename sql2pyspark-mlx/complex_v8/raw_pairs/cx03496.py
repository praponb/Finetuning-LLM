from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_score = Window.orderBy(F.col("score").desc(), "id")
w_grade_status_grade_status_le = Window.partitionBy("grade_status", "grade_status_len").orderBy("grade_status_len", "id")
w_term_course_id_by_score_runn = Window.partitionBy("term", "course_id").orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_student_id_by_score_ties = Window.partitionBy("student_id").orderBy(F.col("score").desc_nulls_first())
w_grade_status_len_course_id = Window.partitionBy("grade_status_len", "course_id")
w_seq_no = Window.partitionBy("seq_no")
w_score_dense_rank = Window.partitionBy("score_dense_rank")

recent = (
    df_enrollments
    .filter((F.col("score").between(32.9, 92.4)) & (F.col("grade_status").isin("FAIL", "PASS")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.concat(F.col("grade_status"), F.lit("#"), F.col("id").cast("string")).alias("grade_status_ref"),
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
    )
)

with_windows = (
    recent
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status_ref",
        "grade_status_len",
        F.lag("score").over(w_all_by_score).alias("prev_score"),
        F.row_number().over(w_grade_status_grade_status_le).alias("seq_no"),
        F.count(F.lit(1)).over(w_term_course_id_by_score_runn).alias("running_count"),
        F.lag("score").over(w_grade_status_grade_status_le).alias("prev_score_2"),
        F.dense_rank().over(w_student_id_by_score_ties).alias("score_dense_rank"),
        F.round(F.col("grade_status_len") - F.avg("grade_status_len").over(w_grade_status_len_course_id), 2).alias("grade_status_len_vs_grade_status_len_course_id_a"),
        F.count(F.lit(1)).over(w_grade_status_len_course_id).alias("rows_in_grade_status_len_course_id"),
        F.lag("score", 2).over(w_grade_status_grade_status_le).alias("prev2_score"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "student_id",
        "grade_status_ref",
        "grade_status_len",
        "prev_score",
        "seq_no",
        "running_count",
        "grade_status_len_vs_grade_status_len_course_id_a",
        "rows_in_grade_status_len_course_id",
        F.sum("score_dense_rank").over(w_seq_no).alias("seq_no_sum_score_dense_rank"),
        F.count(F.lit(1)).over(w_score_dense_rank).alias("rows_in_score_dense_rank"),
        F.count(F.lit(1)).over(w_seq_no).alias("rows_in_seq_no"),
        F.max("running_count").over(w_seq_no).alias("seq_no_max_running_count"),
        F.size(F.collect_set("term").over(w_seq_no)).alias("distinct_term_in_seq_no"),
        F.min("prev_score_2").over(w_score_dense_rank).alias("score_dense_rank_min_prev_score_2"),
    )
)

summary = (
    metrics
    .groupBy("seq_no", "student_id", "distinct_term_in_seq_no")
    .agg(
        F.round(F.sum("grade_status_len_vs_grade_status_len_course_id_a") / F.nullif(F.sum("running_count"), F.lit(0)), 3).alias("grade_status_len_vs_grade_status_len_course_id_a_2"),
        F.round(F.avg("score_dense_rank_min_prev_score_2"), 2).alias("avg_score_dense_rank_min_prev_score_2"),
        F.round(F.stddev("grade_status_len"), 2).alias("stddev_grade_status_len"),
        F.countDistinct("id").alias("distinct_ids"),
        F.max("prev_score").alias("max_prev_score"),
        F.percentile_approx("rows_in_seq_no", 0.5).alias("median_rows_in_seq_no"),
    )
)

result = (
    summary
    .select(
        "seq_no",
        "student_id",
        "distinct_term_in_seq_no",
        "grade_status_len_vs_grade_status_len_course_id_a_2",
        "stddev_grade_status_len",
        "distinct_ids",
        "max_prev_score",
    )
    .orderBy(F.col("distinct_ids").desc(), F.col("seq_no").desc())
)
