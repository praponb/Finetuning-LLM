from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_enrolled_at_ties = Window.orderBy(F.col("enrolled_at").desc())
w_course_id = Window.partitionBy("course_id")
w_all_by_score = Window.orderBy(F.col("score").asc_nulls_last(), "id")
w_all_by_score_ties = Window.orderBy("score")
w_all_by_enrolled_at_moving_2 = Window.orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(-2, Window.currentRow)
w_all_by_enrolled_at_moving_3 = Window.orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(-3, Window.currentRow)
w_all_by_enrolled_at_full = Window.orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_grade_status_score_rank = Window.partitionBy("grade_status", "score_rank")
w_enrolled_at_rank_grade_statu = Window.partitionBy("enrolled_at_rank", "grade_status").orderBy(F.col("score_avg_4").desc_nulls_first(), "id").rowsBetween(-6, Window.currentRow)
w_enrolled_at_rank_grade_statu_2 = Window.partitionBy("enrolled_at_rank", "grade_status").orderBy(F.col("score_avg_4").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_enrolled_at_ties_2 = Window.orderBy("enrolled_at")
w_all_by_enrolled_at = Window.orderBy("enrolled_at", "id")

recent = (
    df_enrollments
    .filter(F.col("grade_status").isin("INCOMPLETE", "WITHDRAWN"))
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

windowed = (
    recent
    .select(
        "*",
        F.rank().over(w_all_by_enrolled_at_ties).alias("enrolled_at_rank"),
        F.size(F.collect_set("grade_status").over(w_course_id)).alias("distinct_grade_status_in_course_id"),
        F.lead("score", 2).over(w_all_by_score).alias("next2_score"),
        F.size(F.collect_set("term").over(w_course_id)).alias("distinct_term_in_course_id"),
        F.round(F.col("score") - F.avg("score").over(w_course_id), 2).alias("score_vs_course_id_avg"),
        F.min("score").over(w_course_id).alias("course_id_min_score"),
        F.rank().over(w_all_by_score_ties).alias("score_rank"),
        F.max("score").over(w_all_by_enrolled_at_moving_2).alias("score_max_3"),
        F.round(F.percent_rank().over(w_all_by_enrolled_at_ties), 3).alias("enrolled_at_percent_rank"),
        F.sum("score").over(w_course_id).alias("course_id_sum_score"),
        F.round(F.avg("score").over(w_all_by_enrolled_at_moving_3), 2).alias("score_avg_4"),
        F.nth_value("score", 2).over(w_all_by_enrolled_at_full).alias("second_score"),
        F.lead("enrolled_at").over(w_all_by_score).alias("next_enrolled_at"),
        F.count(F.lit(1)).over(w_course_id).alias("rows_in_course_id"),
    )
)

with_trends = (
    windowed
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "enrolled_at",
        "enrolled_at_rank",
        "distinct_grade_status_in_course_id",
        "next2_score",
        "distinct_term_in_course_id",
        "score_vs_course_id_avg",
        "course_id_min_score",
        "score_rank",
        "score_max_3",
        "course_id_sum_score",
        "next_enrolled_at",
        "rows_in_course_id",
        F.sum("course_id_sum_score").over(w_grade_status_score_rank).alias("grade_status_score_rank_sum_course_id_sum_score"),
        F.min("score_avg_4").over(w_grade_status_score_rank).alias("grade_status_score_rank_min_score_avg_4"),
        F.sum("enrolled_at_rank").over(w_enrolled_at_rank_grade_statu).alias("enrolled_at_rank_sum_7"),
        F.count(F.lit(1)).over(w_enrolled_at_rank_grade_statu_2).alias("running_count"),
        F.rank().over(w_all_by_enrolled_at_ties_2).alias("enrolled_at_rank_2"),
        F.dense_rank().over(w_all_by_enrolled_at).alias("enrolled_at_dense_rank"),
        F.count(F.lit(1)).over(w_grade_status_score_rank).alias("rows_in_grade_status_score_rank"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "enrolled_at",
        "enrolled_at_rank",
        "distinct_grade_status_in_course_id",
        "next2_score",
        "score_vs_course_id_avg",
        "course_id_min_score",
        "score_rank",
        "score_max_3",
        "grade_status_score_rank_sum_course_id_sum_score",
        "enrolled_at_rank_sum_7",
        "running_count",
        "enrolled_at_rank_2",
        "enrolled_at_dense_rank",
        F.greatest(F.col("enrolled_at_dense_rank"), F.col("course_id_min_score")).alias("greatest_enrolled_at_dense_rank_course_id_min_sc"),
        F.round(F.col("course_id_sum_score") * 1.1, 2).alias("course_id_sum_score_adj"),
    )
    .orderBy(F.col("course_id_min_score").asc())
)
