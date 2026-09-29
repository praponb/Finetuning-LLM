from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status = Window.partitionBy("grade_status")
w_enrolled_at_month_by_score = Window.partitionBy(F.date_trunc("month", "enrolled_at")).orderBy("score", "id")
w_grade_status_course_id_by_sc = Window.partitionBy("grade_status", "course_id").orderBy(F.col("score").desc(), "id").rowsBetween(-2, Window.currentRow)
w_all_by_score = Window.orderBy("score", "id")
w_grade_status_course_id_by_sc_2 = Window.partitionBy("grade_status", "course_id").orderBy(F.col("score").desc(), "id")
w_enrolled_at_year_term_by_sco = Window.partitionBy(F.year("enrolled_at"), "term").orderBy("score", "id")
w_grade_status_term = Window.partitionBy("grade_status", "term")
w_grade_status_by_score_rank = Window.partitionBy("grade_status").orderBy(F.col("score_rank").asc_nulls_last(), "id")
w_all_by_prev_enrolled_at_movi = Window.orderBy(F.col("prev_enrolled_at").desc(), "id").rowsBetween(-3, Window.currentRow)
w_all_by_prev_enrolled_at_runn = Window.orderBy(F.col("prev_enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_score_vs_grade_status = Window.orderBy("score_vs_grade_status_term_avg")

valid_enrollments = (
    df_enrollments
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
    valid_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.count(F.lit(1)).over(w_grade_status).alias("rows_in_grade_status"),
        F.ntile(3).over(w_enrolled_at_month_by_score).alias("score_tercile"),
        F.sum("score").over(w_grade_status_course_id_by_sc).alias("score_sum_3"),
        F.ntile(3).over(w_all_by_score).alias("score_tercile_2"),
        F.lag("enrolled_at").over(w_grade_status_course_id_by_sc_2).alias("prev_enrolled_at"),
        F.lead("enrolled_at").over(w_enrolled_at_year_term_by_sco).alias("next_enrolled_at"),
        F.row_number().over(w_enrolled_at_year_term_by_sco).alias("score_seq"),
        F.rank().over(w_grade_status_course_id_by_sc_2).alias("score_rank"),
        F.round(F.col("score") - F.avg("score").over(w_grade_status_term), 2).alias("score_vs_grade_status_term_avg"),
        F.min("score").over(w_grade_status_term).alias("grade_status_term_min_score"),
    )
)

metrics_2 = (
    metrics
    .select(
        "id",
        "student_id",
        "term",
        "score",
        "grade_status",
        "rows_in_grade_status",
        "score_tercile",
        "score_sum_3",
        "score_tercile_2",
        "prev_enrolled_at",
        "next_enrolled_at",
        "grade_status_term_min_score",
        (F.col("grade_status_term_min_score") - F.lag("grade_status_term_min_score").over(w_grade_status_by_score_rank)).alias("grade_status_term_min_score_change"),
        F.round(F.avg("grade_status_term_min_score").over(w_all_by_prev_enrolled_at_movi), 2).alias("grade_status_term_min_score_avg_4"),
        F.first("next_enrolled_at", ignorenulls=True).over(w_all_by_prev_enrolled_at_runn).alias("first_known_next_enrolled_at"),
        F.round(F.percent_rank().over(w_all_by_score_vs_grade_status), 3).alias("score_vs_grade_status_term_avg_percent_rank"),
    )
)

result = (
    metrics_2
    .select(
        "id",
        "student_id",
        "score",
        "grade_status",
        "rows_in_grade_status",
        "score_tercile",
        "score_sum_3",
        "score_tercile_2",
        "grade_status_term_min_score_change",
        "grade_status_term_min_score_avg_4",
        "score_vs_grade_status_term_avg_percent_rank",
        F.abs(F.col("rows_in_grade_status") - 10).alias("rows_in_grade_status_distance"),
        F.greatest(F.col("score_tercile"), F.col("score")).alias("greatest_score_tercile_score"),
    )
    .orderBy(F.col("greatest_score_tercile_score").desc(), "id")
    .limit(20)
)
