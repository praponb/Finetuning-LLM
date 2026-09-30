from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_len_term_by_gra = Window.partitionBy("grade_status_len", "term").orderBy("grade_status_len", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grade_status_by_grade_status = Window.partitionBy("grade_status").orderBy("grade_status_len", "id")
w_term_enrolled_at_year = Window.partitionBy("term", F.year("enrolled_at"))
w_grade_status_by_grade_status_2 = Window.partitionBy("grade_status").orderBy("grade_status_len")
w_all_by_grade_status_len_ties = Window.orderBy(F.col("grade_status_len").desc())
w_grade_status_student_id_by_e = Window.partitionBy("grade_status", "student_id").orderBy(F.col("enrolled_at").asc_nulls_last(), "id").rowsBetween(-1, -1)
w_student_id_grade_status_len = Window.partitionBy("student_id", "grade_status_len")
w_all_by_grade_status_len = Window.orderBy(F.col("grade_status_len").desc(), "id")

filtered = (
    df_enrollments
    .filter(F.col("grade_status") != "FAIL")
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
    )
)

windowed = (
    filtered
    .select(
        "*",
        F.min("grade_status_len").over(w_grade_status_len_term_by_gra).alias("min_grade_status_len_so_far"),
        F.lead("grade_status_len").over(w_grade_status_by_grade_status).alias("next_grade_status_len"),
        F.sum("score").over(w_term_enrolled_at_year).alias("term_enrolled_at_year_sum_score"),
        F.row_number().over(w_grade_status_by_grade_status).alias("seq_no"),
        F.round(F.percent_rank().over(w_grade_status_by_grade_status_2), 3).alias("grade_status_len_percent_rank"),
        F.round(F.col("score") - F.avg("score").over(w_term_enrolled_at_year), 2).alias("score_vs_term_enrolled_at_year_avg"),
        F.rank().over(w_all_by_grade_status_len_ties).alias("grade_status_len_rank"),
        F.round(F.avg("grade_status_len").over(w_grade_status_student_id_by_e), 2).alias("grade_status_len_avg_1_prior"),
        F.round(F.avg("grade_status_len").over(w_student_id_grade_status_len), 2).alias("student_id_grade_status_len_avg_grade_status_len"),
        F.round(F.cume_dist().over(w_all_by_grade_status_len), 3).alias("grade_status_len_cume_dist"),
    )
)

result = (
    windowed
    .filter(F.col("min_grade_status_len_so_far") <= 85)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        "grade_status_len",
        "min_grade_status_len_so_far",
        "next_grade_status_len",
        "term_enrolled_at_year_sum_score",
        "seq_no",
        "score_vs_term_enrolled_at_year_avg",
        "grade_status_len_rank",
        "grade_status_len_avg_1_prior",
        "student_id_grade_status_len_avg_grade_status_len",
        "grade_status_len_cume_dist",
        F.round(F.col("next_grade_status_len") / F.nullif(F.col("grade_status_len"), F.lit(0)), 3).alias("next_grade_status_len_per_grade_status_len"),
    )
    .orderBy(F.col("student_id_grade_status_len_avg_grade_status_len").desc(), "id")
    .limit(5)
)
