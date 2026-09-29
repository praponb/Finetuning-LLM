from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term_course_id_by_score = Window.partitionBy("term", "course_id").orderBy("score", "id")
w_course_id = Window.partitionBy("course_id")
w_grade_status_term_by_score_m = Window.partitionBy("grade_status", "term").orderBy("score", "id").rowsBetween(-1, Window.currentRow)
w_term_student_id_by_score_mov = Window.partitionBy("term", "student_id").orderBy(F.col("score").desc(), "id").rowsBetween(-6, Window.currentRow)
w_term_course_id_by_score_ties = Window.partitionBy("term", "course_id").orderBy("score")
w_grade_status_term_by_score_f = Window.partitionBy("grade_status", "term").orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_student_id_grade_status = Window.partitionBy("student_id", "grade_status")

cleaned_enrollments = (
    df_enrollments
    .filter((F.col("score") <= 57.8) & (F.col("term") == "2025T2"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
    )
)

ranked = (
    cleaned_enrollments
    .select(
        "*",
        F.round(F.percent_rank().over(w_term_course_id_by_score), 3).alias("score_percent_rank"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_course_id), F.lit(0)), 4).alias("score_share_of_course_id"),
        F.max("score").over(w_grade_status_term_by_score_m).alias("score_max_2"),
        F.round(F.avg("score").over(w_term_student_id_by_score_mov), 2).alias("score_avg_7"),
        F.rank().over(w_term_course_id_by_score_ties).alias("score_rank"),
        F.round(F.avg("score").over(w_course_id), 2).alias("course_id_avg_score"),
        F.last("score").over(w_grade_status_term_by_score_f).alias("last_score"),
        F.max("score").over(w_student_id_grade_status).alias("student_id_grade_status_max_score"),
    )
)

top_rows = ranked.filter(F.col("score_rank") == 1)

result = (
    top_rows
    .filter(F.col("student_id") >= 14)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score_percent_rank",
        "score_share_of_course_id",
        "score_max_2",
        "score_avg_7",
        "score_rank",
        "last_score",
    )
    .orderBy(F.col("score_avg_7").desc_nulls_first(), F.col("last_score").asc(), "id")
    .limit(5)
)
