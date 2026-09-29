from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_student_gpa_prior_3 = Window.orderBy(F.col("student_gpa").asc_nulls_last(), "id").rowsBetween(-3, -1)
w_student_id_term_by_score_tie = Window.partitionBy("student_id", "term").orderBy("score")
w_student_id_term_by_score = Window.partitionBy("student_id", "term").orderBy("score", "id")
w_all_by_student_gpa_moving_1 = Window.orderBy(F.col("student_gpa").asc_nulls_last(), "id").rowsBetween(-1, Window.currentRow)
w_student_id_term_by_score_run = Window.partitionBy("student_id", "term").orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_student_id_term_by_score_pri = Window.partitionBy("student_id", "term").orderBy("score", "id").rowsBetween(-1, -1)
w_course_id_by_student_gpa = Window.partitionBy("course_id").orderBy(F.col("student_gpa").desc(), "id")

scoped_enrollments = (
    df_enrollments
    .filter(F.col("student_id") >= 2)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.year(F.col("enrolled_at")).alias("enrolled_at_year"),
        F.concat(F.col("grade_status"), F.lit("#"), F.col("course_id").cast("string")).alias("grade_status_ref"),
    )
)

with_parent = (
    scoped_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.*",
        F.col("st.is_international").alias("student_is_international"),
        F.col("st.gpa").alias("student_gpa"),
    )
)

windowed = (
    with_parent
    .select(
        "*",
        F.max("enrolled_at_year").over(w_all_by_student_gpa_prior_3).alias("enrolled_at_year_max_3_prior"),
        F.round(F.percent_rank().over(w_student_id_term_by_score_tie), 3).alias("score_percent_rank"),
        (F.col("student_gpa") - F.lag("student_gpa").over(w_student_id_term_by_score)).alias("student_gpa_change"),
        F.round(F.avg("score").over(w_all_by_student_gpa_moving_1), 2).alias("score_avg_2"),
        F.min("student_gpa").over(w_student_id_term_by_score_run).alias("min_student_gpa_so_far"),
        F.max("enrolled_at_year").over(w_student_id_term_by_score_pri).alias("enrolled_at_year_max_1_prior"),
        (F.col("student_gpa") - F.lag("student_gpa").over(w_course_id_by_student_gpa)).alias("student_gpa_change_2"),
        F.round(F.cume_dist().over(w_student_id_term_by_score_tie), 3).alias("score_cume_dist"),
    )
)

result = (
    windowed
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        "enrolled_at_year",
        "grade_status_ref",
        "student_is_international",
        "student_gpa",
        "enrolled_at_year_max_3_prior",
        "score_percent_rank",
        "student_gpa_change",
        "score_avg_2",
        "student_gpa_change_2",
    )
    .orderBy(F.col("score_percent_rank").asc(), F.col("score_avg_2").desc())
)
