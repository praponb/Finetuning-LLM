from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term_course_id_by_score_full = Window.partitionBy("term", "course_id").orderBy(F.col("score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_id_student_id_by_scor = Window.partitionBy("course_id", "student_id").orderBy("score", "id")
w_grade_status = Window.partitionBy("grade_status")
w_term_grade_status = Window.partitionBy("term", "grade_status")
w_term_course_id_by_score_ties = Window.partitionBy("term", "course_id").orderBy(F.col("score").desc())
w_all_by_score_prior_6 = Window.orderBy(F.col("score").desc(), "id").rowsBetween(-6, -1)
w_all_by_score = Window.orderBy(F.col("score").desc(), "id")
w_course_id_student_id_by_scor_2 = Window.partitionBy("course_id", "student_id").orderBy("score")
w_course_id_by_score_percent_r = Window.partitionBy("course_id").orderBy("score_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_grade_status_group = Window.partitionBy("distinct_grade_status_group_in_term_grade_status")
w_all_by_distinct_grade_status = Window.orderBy(F.col("distinct_grade_status_group_in_term_grade_status").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_id_by_score_percent_r_2 = Window.partitionBy("course_id").orderBy("score_percent_rank", "id").rowsBetween(-1, Window.currentRow)
w_course_id_by_score_percent_r_3 = Window.partitionBy("course_id").orderBy("score_percent_rank")
w_score_percent_rank_dense_ran = Window.partitionBy("score_percent_rank_dense_rank", "grade_status_group").orderBy("score_seq_term_2024t2")
w_cnt_distinct_grade_status_gr = Window.partitionBy("cnt", "distinct_grade_status_group_in_term_grade_status")

valid = (
    df_enrollments
    .filter(F.col("grade_status").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.when(F.col("grade_status").isin("WITHDRAWN", "INCOMPLETE"), "group_a").when(F.col("grade_status") == "PASS", "group_b").otherwise("other").alias("grade_status_group"),
    )
)

ranked = (
    valid
    .select(
        "*",
        F.nth_value("score", 2).over(w_term_course_id_by_score_full).alias("second_score"),
        F.dense_rank().over(w_course_id_student_id_by_scor).alias("score_dense_rank"),
        F.count(F.lit(1)).over(w_grade_status).alias("rows_in_grade_status"),
        F.min("score").over(w_term_grade_status).alias("term_grade_status_min_score"),
        F.round(F.col("score") - F.avg("score").over(w_grade_status), 2).alias("score_vs_grade_status_avg"),
        F.round(F.percent_rank().over(w_term_course_id_by_score_ties), 3).alias("score_percent_rank"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_grade_status), F.lit(0)), 4).alias("score_share_of_grade_status"),
        F.round(F.avg("score").over(w_all_by_score_prior_6), 2).alias("score_avg_6_prior"),
        F.sum("score").over(w_grade_status).alias("grade_status_sum_score"),
        F.size(F.collect_set("grade_status_group").over(w_term_grade_status)).alias("distinct_grade_status_group_in_term_grade_status"),
        F.row_number().over(w_all_by_score).alias("score_seq"),
        F.dense_rank().over(w_course_id_student_id_by_scor_2).alias("score_dense_rank_2"),
    )
)

sequenced = (
    ranked
    .select(
        "id",
        "student_id",
        "term",
        "grade_status",
        "grade_status_group",
        "second_score",
        "score_dense_rank",
        "rows_in_grade_status",
        "term_grade_status_min_score",
        "score_vs_grade_status_avg",
        "score_percent_rank",
        "score_share_of_grade_status",
        "distinct_grade_status_group_in_term_grade_status",
        "score_seq",
        F.nth_value("second_score", 2).over(w_course_id_by_score_percent_r).alias("second_second_score"),
        F.round(F.avg("grade_status_sum_score").over(w_distinct_grade_status_group), 2).alias("distinct_grade_status_group_in_term_grade_status_2"),
        F.nth_value("score_percent_rank", 2).over(w_course_id_by_score_percent_r).alias("second_score_percent_rank"),
        F.last("term_grade_status_min_score").over(w_all_by_distinct_grade_status).alias("last_term_grade_status_min_score"),
        F.round(F.avg("term_grade_status_min_score").over(w_course_id_by_score_percent_r_2), 2).alias("term_grade_status_min_score_avg_2"),
        F.dense_rank().over(w_course_id_by_score_percent_r_3).alias("score_percent_rank_dense_rank"),
    )
)

totals = (
    sequenced
    .groupBy(
        "grade_status_group",
        "distinct_grade_status_group_in_term_grade_status",
        "score_percent_rank_dense_rank",
    )
    .agg(
        F.sum(F.when(F.col("term") != "2024T2", F.col("score_seq"))).alias("score_seq_term_2024t2"),
        F.max("rows_in_grade_status").alias("max_rows_in_grade_status"),
        F.count("*").alias("cnt"),
        F.array_sort(F.collect_set("grade_status")).alias("grade_status_list"),
    )
    .filter(F.col("cnt") >= 2)
)

with_trends = (
    totals
    .select(
        "grade_status_group",
        "distinct_grade_status_group_in_term_grade_status",
        "score_percent_rank_dense_rank",
        "score_seq_term_2024t2",
        "max_rows_in_grade_status",
        "cnt",
        "grade_status_list",
        F.round(F.percent_rank().over(w_score_percent_rank_dense_ran), 3).alias("score_seq_term_2024t2_percent_rank"),
        F.max("score_seq_term_2024t2").over(w_cnt_distinct_grade_status_gr).alias("cnt_distinct_grade_status_group_in_term_grade_st"),
        F.count(F.lit(1)).over(w_cnt_distinct_grade_status_gr).alias("rows_in_cnt_distinct_grade_status_group_in_term_"),
    )
)

result = (
    with_trends
    .select(
        "grade_status_group",
        "score_percent_rank_dense_rank",
        "max_rows_in_grade_status",
        "cnt",
        "score_seq_term_2024t2_percent_rank",
        "cnt_distinct_grade_status_group_in_term_grade_st",
        "rows_in_cnt_distinct_grade_status_group_in_term_",
        F.coalesce(F.col("score_seq_term_2024t2"), F.lit(0)).alias("score_seq_term_2024t2_filled"),
        F.coalesce(F.col("grade_status_group"), F.lit("UNKNOWN")).alias("grade_status_group_or_unknown"),
    )
    .orderBy(
        "score_percent_rank_dense_rank",
        F.col("grade_status_group_or_unknown").asc_nulls_last(),
    )
)
