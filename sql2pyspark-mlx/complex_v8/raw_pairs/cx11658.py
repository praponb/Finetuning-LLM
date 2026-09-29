from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_score = Window.orderBy("score", "id")
w_grade_status_course_id_by_sc = Window.partitionBy("grade_status", "course_id").orderBy(F.col("score").desc(), "id")
w_student_id_course_id_by_enro = Window.partitionBy("student_id", "course_id").orderBy("enrolled_at", "id")
w_grade_status_course_id_by_sc_2 = Window.partitionBy("grade_status", "course_id").orderBy(F.col("score").desc())
w_grade_status = Window.partitionBy("grade_status")
w_all_by_enrolled_at = Window.orderBy(F.col("enrolled_at").desc(), "id")
w_student_id_enrolled_at_to_da = Window.partitionBy("student_id", F.to_date("enrolled_at"))
w_term_course_id_by_enrolled_a = Window.partitionBy("term", "course_id").orderBy("enrolled_at")
w_term_course_id_by_enrolled_a_2 = Window.partitionBy("term", "course_id").orderBy("enrolled_at", "id")
w_all_by_enrolled_at_dense_ran = Window.orderBy(F.col("enrolled_at_dense_rank").desc(), "id")
w_rn_enrolled_at_dense_rank = Window.partitionBy("rn", "enrolled_at_dense_rank")
w_all_by_enrolled_at_dense_ran_2 = Window.orderBy(F.col("enrolled_at_dense_rank").desc(), "id").rowsBetween(-1, -1)
w_all_by_enrolled_at_dense_ran_3 = Window.orderBy(F.col("enrolled_at_dense_rank").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_enrolled_at_dense_ran_4 = Window.orderBy(F.col("enrolled_at_dense_rank").desc())
w_all_by_next_score_moving_1 = Window.orderBy(F.col("next_score").desc_nulls_first(), "id").rowsBetween(-1, Window.currentRow)

filtered_enrollments = (
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
    )
)

metrics = (
    filtered_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.row_number().over(w_all_by_score).alias("score_seq"),
        F.lead("score").over(w_grade_status_course_id_by_sc).alias("next_score"),
        F.ntile(4).over(w_student_id_course_id_by_enro).alias("enrolled_at_quartile"),
        F.dense_rank().over(w_grade_status_course_id_by_sc_2).alias("score_dense_rank"),
        (F.col("score") - F.lag("score").over(w_student_id_course_id_by_enro)).alias("score_change"),
        F.size(F.collect_set("course_id").over(w_grade_status)).alias("distinct_course_id_in_grade_status"),
        F.row_number().over(w_all_by_enrolled_at).alias("rn"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_student_id_enrolled_at_to_da), F.lit(0)), 4).alias("score_share_of_student_id_enrolled_at_to_date"),
        F.count(F.lit(1)).over(w_student_id_enrolled_at_to_da).alias("rows_in_student_id_enrolled_at_to_date"),
        F.dense_rank().over(w_term_course_id_by_enrolled_a).alias("enrolled_at_dense_rank"),
        F.row_number().over(w_term_course_id_by_enrolled_a_2).alias("seq_no"),
        F.count(F.lit(1)).over(w_grade_status).alias("rows_in_grade_status"),
        F.size(F.collect_set("course_id").over(w_student_id_enrolled_at_to_da)).alias("distinct_course_id_in_student_id_enrolled_at_to_"),
        F.min("score").over(w_grade_status).alias("grade_status_min_score"),
        F.round(F.percent_rank().over(w_all_by_score), 3).alias("score_percent_rank"),
        F.lead("enrolled_at", 2).over(w_term_course_id_by_enrolled_a_2).alias("next2_enrolled_at"),
    )
)

metrics_2 = (
    metrics
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "next_score",
        "score_dense_rank",
        "distinct_course_id_in_grade_status",
        "rn",
        "score_share_of_student_id_enrolled_at_to_date",
        "enrolled_at_dense_rank",
        "seq_no",
        "distinct_course_id_in_student_id_enrolled_at_to_",
        "score_percent_rank",
        "next2_enrolled_at",
        F.rank().over(w_all_by_enrolled_at_dense_ran).alias("enrolled_at_dense_rank_rank"),
        F.min("score_percent_rank").over(w_rn_enrolled_at_dense_rank).alias("rn_enrolled_at_dense_rank_min_score_percent_rank"),
        F.round(F.avg("rows_in_grade_status").over(w_all_by_enrolled_at_dense_ran_2), 2).alias("rows_in_grade_status_avg_1_prior"),
        F.last("enrolled_at_quartile").over(w_all_by_enrolled_at_dense_ran_3).alias("last_enrolled_at_quartile"),
        F.round(F.cume_dist().over(w_all_by_enrolled_at_dense_ran_4), 3).alias("enrolled_at_dense_rank_cume_dist"),
        F.size(F.collect_set("course_id").over(w_rn_enrolled_at_dense_rank)).alias("distinct_course_id_in_rn_enrolled_at_dense_rank"),
        F.sum("score_seq").over(w_all_by_next_score_moving_1).alias("score_seq_sum_2"),
        F.size(F.collect_set("term").over(w_rn_enrolled_at_dense_rank)).alias("distinct_term_in_rn_enrolled_at_dense_rank"),
    )
)

result = (
    metrics_2
    .select(
        "id",
        "student_id",
        "course_id",
        "score_dense_rank",
        "distinct_course_id_in_grade_status",
        "rn",
        "score_share_of_student_id_enrolled_at_to_date",
        "enrolled_at_dense_rank",
        "distinct_course_id_in_student_id_enrolled_at_to_",
        "score_percent_rank",
        "next2_enrolled_at",
        "enrolled_at_dense_rank_rank",
        "rows_in_grade_status_avg_1_prior",
        "last_enrolled_at_quartile",
        "enrolled_at_dense_rank_cume_dist",
        "distinct_course_id_in_rn_enrolled_at_dense_rank",
        "score_seq_sum_2",
        "distinct_term_in_rn_enrolled_at_dense_rank",
    )
    .orderBy(
        "score_percent_rank",
        F.col("enrolled_at_dense_rank_cume_dist").desc_nulls_first(),
    )
)
