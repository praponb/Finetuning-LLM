from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term_course_id = Window.partitionBy("term", "course_id")
w_course_id_term = Window.partitionBy("course_id", "term")
w_all_by_enrolled_at_full = Window.orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_student_id_by_score_ties = Window.partitionBy("student_id").orderBy("score")
w_all_by_enrolled_at = Window.orderBy("enrolled_at", "id")
w_term_student_id_by_score_tie = Window.partitionBy("term", "student_id").orderBy(F.col("score").asc_nulls_last())
w_student_id_by_enrolled_at_mo = Window.partitionBy("student_id").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(-2, Window.currentRow)
w_student_id_enrolled_at_seq_b = Window.partitionBy("student_id", "enrolled_at_seq").orderBy("enrolled_at_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_score_dense_rank = Window.partitionBy("score_dense_rank")
w_score_dense_rank_by_score_de = Window.partitionBy("score_dense_rank").orderBy("score_dense_rank")

valid = (
    df_enrollments
    .filter((F.col("term") == "2025T2") & (F.col("grade_status") == "FAIL"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
    )
)

with_windows = (
    valid
    .select(
        "*",
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_term_course_id), F.lit(0)), 4).alias("score_share_of_term_course_id"),
        F.round(F.avg("score").over(w_term_course_id), 2).alias("term_course_id_avg_score"),
        F.count(F.lit(1)).over(w_course_id_term).alias("rows_in_course_id_term"),
        F.nth_value("enrolled_at", 2).over(w_all_by_enrolled_at_full).alias("second_enrolled_at"),
        F.dense_rank().over(w_student_id_by_score_ties).alias("score_dense_rank"),
        F.round(F.col("score") - F.avg("score").over(w_term_course_id), 2).alias("score_vs_term_course_id_avg"),
        F.lead("enrolled_at", 3).over(w_all_by_enrolled_at).alias("next3_enrolled_at"),
        F.rank().over(w_term_student_id_by_score_tie).alias("score_rank"),
        F.row_number().over(w_all_by_enrolled_at).alias("enrolled_at_seq"),
    )
)

leaders = (
    with_windows
    .filter(F.col("score_rank") == 1)
    .drop("score_rank")
)

sequenced = (
    leaders
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "score_share_of_term_course_id",
        "score_dense_rank",
        "score_vs_term_course_id_avg",
        "next3_enrolled_at",
        F.max("score").over(w_student_id_by_enrolled_at_mo).alias("score_max_3"),
        F.last("enrolled_at_seq", ignorenulls=True).over(w_student_id_enrolled_at_seq_b).alias("last_known_enrolled_at_seq"),
        F.min("score").over(w_score_dense_rank).alias("score_dense_rank_min_score"),
        F.dense_rank().over(w_score_dense_rank_by_score_de).alias("score_dense_rank_dense_rank"),
        F.size(F.collect_set("term").over(w_score_dense_rank)).alias("distinct_term_in_score_dense_rank"),
        F.round(F.col("score_vs_term_course_id_avg") - F.avg("score_vs_term_course_id_avg").over(w_score_dense_rank), 2).alias("score_vs_term_course_id_avg_vs_score_dense_rank_"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "student_id",
        "term",
        "score",
        "score_dense_rank",
        "next3_enrolled_at",
        "score_max_3",
        "last_known_enrolled_at_seq",
        "score_dense_rank_dense_rank",
        "distinct_term_in_score_dense_rank",
        "score_vs_term_course_id_avg_vs_score_dense_rank_",
        F.coalesce(F.col("score_dense_rank"), F.lit(0)).alias("score_dense_rank_filled"),
    )
    .orderBy("distinct_term_in_score_dense_rank")
)
