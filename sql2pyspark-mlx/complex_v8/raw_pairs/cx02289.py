from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id_grade_status = Window.partitionBy("student_id", "grade_status")
w_student_id_term = Window.partitionBy("student_id", "term")
w_all_by_score_filled_ties = Window.orderBy(F.col("score_filled").desc())
w_all_by_score_ties = Window.orderBy("score")
w_grade_status_student_id = Window.partitionBy("grade_status", "student_id")
w_all_by_score_filled_moving_6 = Window.orderBy(F.col("score_filled").desc(), "id").rowsBetween(-6, Window.currentRow)
w_term = Window.partitionBy("term")
w_all_by_score_filled_share_of = Window.orderBy(F.col("score_filled_share_of_student_id_term").asc_nulls_last(), "id")
w_all_by_score_filled_full = Window.orderBy(F.col("score_filled").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_score_filled_running = Window.orderBy(F.col("score_filled").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_rows_in_student_id_gr = Window.orderBy(F.col("rows_in_student_id_grade_status").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped = (
    df_enrollments
    .filter((F.col("grade_status").isin("FAIL", "INCOMPLETE", "PASS")) & (F.col("student_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        (F.col("enrolled_at") + F.expr("INTERVAL 1 DAYS")).alias("enrolled_at_plus_1d"),
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
    )
)

ranked = (
    scoped
    .select(
        "*",
        F.count(F.lit(1)).over(w_student_id_grade_status).alias("rows_in_student_id_grade_status"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_student_id_grade_status), F.lit(0)), 4).alias("score_share_of_student_id_grade_status"),
        F.max("score").over(w_student_id_grade_status).alias("student_id_grade_status_max_score"),
        F.min("score_filled").over(w_student_id_grade_status).alias("student_id_grade_status_min_score_filled"),
        F.round(F.col("score_filled") / F.nullif(F.sum("score_filled").over(w_student_id_term), F.lit(0)), 4).alias("score_filled_share_of_student_id_term"),
        F.size(F.collect_set("term").over(w_student_id_grade_status)).alias("distinct_term_in_student_id_grade_status"),
        F.round(F.percent_rank().over(w_all_by_score_filled_ties), 3).alias("score_filled_percent_rank"),
        F.round(F.col("score") - F.avg("score").over(w_student_id_term), 2).alias("score_vs_student_id_term_avg"),
        F.rank().over(w_all_by_score_ties).alias("score_rank"),
        F.size(F.collect_set("term").over(w_grade_status_student_id)).alias("distinct_term_in_grade_status_student_id"),
        F.max("score").over(w_all_by_score_filled_moving_6).alias("score_max_7"),
    )
)

metrics = (
    ranked
    .select(
        "*",
        F.round(F.col("score_filled_percent_rank") / F.nullif(F.sum("score_filled_percent_rank").over(w_term), F.lit(0)), 4).alias("score_filled_percent_rank_share_of_term"),
        F.row_number().over(w_all_by_score_filled_share_of).alias("rn"),
        F.nth_value("score_max_7", 2).over(w_all_by_score_filled_full).alias("second_score_max_7"),
        F.min("score_max_7").over(w_all_by_score_filled_running).alias("min_score_max_7_so_far"),
        F.first("score_vs_student_id_term_avg").over(w_all_by_rows_in_student_id_gr).alias("first_score_vs_student_id_term_avg"),
    )
)

result = (
    metrics
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at_plus_1d",
        "score_filled",
        "score_share_of_student_id_grade_status",
        "student_id_grade_status_max_score",
        "student_id_grade_status_min_score_filled",
        "score_filled_share_of_student_id_term",
        "distinct_term_in_student_id_grade_status",
        "score_vs_student_id_term_avg",
        "score_rank",
        "distinct_term_in_grade_status_student_id",
        "score_max_7",
        "score_filled_percent_rank_share_of_term",
        "second_score_max_7",
        "min_score_max_7_so_far",
        "first_score_vs_student_id_term_avg",
    )
    .orderBy("grade_status", "id")
    .limit(5)
)
