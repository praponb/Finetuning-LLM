from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term_by_score = Window.partitionBy("term").orderBy(F.col("score").asc_nulls_last(), "id")
w_course_level_by_score_ties = Window.partitionBy("course_level").orderBy(F.col("score").asc_nulls_last())
w_student_is_international = Window.partitionBy("student_is_international")
w_term_by_score_filled_ties = Window.partitionBy("term").orderBy(F.col("score_filled").desc())
w_term_by_score_running = Window.partitionBy("term").orderBy(F.col("score").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_course_level_by_score = Window.partitionBy("course_level").orderBy(F.col("score").asc_nulls_last(), "id")
w_term_by_score_filled = Window.partitionBy("term").orderBy(F.col("score_filled").desc(), "id")
w_term_by_score_moving_1 = Window.partitionBy("term").orderBy(F.col("score").asc_nulls_last(), "id").rowsBetween(-1, Window.currentRow)
w_score_filled_seq_grade_statu = Window.partitionBy("score_filled_seq", "grade_status").orderBy("score_vs_student_is_international_avg", "id").rowsBetween(-3, Window.currentRow)
w_score_filled_seq_grade_statu_2 = Window.partitionBy("score_filled_seq", "grade_status").orderBy("score_vs_student_is_international_avg", "id")
w_score_dense_rank_student_id = Window.partitionBy("score_dense_rank", "student_id")

filtered_enrollments = (
    df_enrollments
    .filter(F.col("term").isin("2024T1", "2024T2", "2025T1"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.date_format(F.col("enrolled_at"), "MMM yyyy").alias("enrolled_at_label"),
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
    )
)

with_parent = (
    filtered_enrollments.alias("s")
    .join(df_courses.alias("c"), (F.col("c.id") == F.col("s.course_id")) & (F.col("c.level").isNotNull()), "left")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.enrolled_at_label",
        "s.score_filled",
        F.col("c.level").alias("course_level"),
        F.col("st.is_international").alias("student_is_international"),
        F.col("st.gpa").alias("student_gpa"),
    )
)

with_windows = (
    with_parent
    .select(
        "*",
        F.lag("student_gpa", 2).over(w_term_by_score).alias("prev2_student_gpa"),
        F.round(F.cume_dist().over(w_course_level_by_score_ties), 3).alias("score_cume_dist"),
        F.count(F.lit(1)).over(w_student_is_international).alias("rows_in_student_is_international"),
        F.round(F.col("score") - F.avg("score").over(w_student_is_international), 2).alias("score_vs_student_is_international_avg"),
        F.round(F.cume_dist().over(w_term_by_score_filled_ties), 3).alias("score_filled_cume_dist"),
        F.first("score", ignorenulls=True).over(w_term_by_score_running).alias("first_known_score"),
        F.sum("score_filled").over(w_student_is_international).alias("student_is_international_sum_score_filled"),
        F.ntile(10).over(w_course_level_by_score).alias("score_decile"),
        F.lead("score", 1, 0).over(w_term_by_score).alias("next_score"),
        F.row_number().over(w_term_by_score_filled).alias("score_filled_seq"),
        F.dense_rank().over(w_course_level_by_score_ties).alias("score_dense_rank"),
        F.sum("student_gpa").over(w_term_by_score_moving_1).alias("student_gpa_sum_2"),
        F.sum("score").over(w_student_is_international).alias("student_is_international_sum_score"),
    )
)

top_rows = with_windows.filter((F.col("score_filled_seq") <= 5) & (F.col("student_id").isNotNull()))

ranked = (
    top_rows
    .select(
        "*",
        F.sum("student_gpa_sum_2").over(w_score_filled_seq_grade_statu).alias("student_gpa_sum_2_sum_4"),
        F.ntile(3).over(w_score_filled_seq_grade_statu_2).alias("score_vs_student_is_international_avg_tercile"),
        F.sum("score_filled_cume_dist").over(w_score_dense_rank_student_id).alias("score_dense_rank_student_id_sum_score_filled_cum"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_score_dense_rank_student_id), F.lit(0)), 4).alias("score_share_of_score_dense_rank_student_id"),
    )
)

result = (
    ranked
    .select(
        "id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at_label",
        "course_level",
        "student_gpa",
        "prev2_student_gpa",
        "score_cume_dist",
        "rows_in_student_is_international",
        "score_vs_student_is_international_avg",
        "score_filled_cume_dist",
        "first_known_score",
        "student_is_international_sum_score_filled",
        "score_decile",
        "next_score",
        "score_dense_rank",
        "student_gpa_sum_2",
        "student_is_international_sum_score",
        "student_gpa_sum_2_sum_4",
        "score_vs_student_is_international_avg_tercile",
        "score_dense_rank_student_id_sum_score_filled_cum",
        "score_share_of_score_dense_rank_student_id",
        (F.col("next_score") + F.col("score_decile")).alias("next_score_plus_score_decile"),
    )
)
