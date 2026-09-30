from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id_by_score_ties = Window.partitionBy("student_id").orderBy(F.col("score").desc())
w_student_id_by_score_full = Window.partitionBy("student_id").orderBy(F.col("score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_id = Window.partitionBy("course_id")
w_student_id_by_score = Window.partitionBy("student_id").orderBy(F.col("score").desc(), "id")
w_student_id_course_id_by_scor = Window.partitionBy("student_id", "course_id").orderBy("score", "id")
w_all_by_score_percent_rank = Window.orderBy(F.col("score_percent_rank").desc(), "id")
w_score_dense_rank_term = Window.partitionBy("score_dense_rank", "term")

valid = (
    df_enrollments
    .filter((F.col("enrolled_at") >= F.lit("2025-01-25 13:00:00").cast("timestamp")) & (F.col("student_id") >= 6))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
    )
)

with_trends = (
    valid
    .select(
        "*",
        F.round(F.percent_rank().over(w_student_id_by_score_ties), 3).alias("score_percent_rank"),
        F.dense_rank().over(w_student_id_by_score_ties).alias("score_dense_rank"),
        F.nth_value("score", 2).over(w_student_id_by_score_full).alias("second_score"),
        F.size(F.collect_set("student_id").over(w_course_id)).alias("distinct_student_id_in_course_id"),
        F.min("score").over(w_course_id).alias("course_id_min_score"),
        F.lead("enrolled_at", 2).over(w_student_id_by_score).alias("next2_enrolled_at"),
    )
)

combined_details_course = (
    with_trends.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.enrolled_at",
        "s.score_percent_rank",
        "s.score_dense_rank",
        "s.second_score",
        "s.next2_enrolled_at",
        "c.course_code",
        F.col("c.level").alias("course_level"),
        F.col("st.enrolled_on").alias("student_enrolled_on"),
    )
)

windowed = (
    combined_details_course
    .select(
        "id",
        "score",
        "enrolled_at",
        "score_percent_rank",
        "next2_enrolled_at",
        "course_code",
        "student_enrolled_on",
        F.lag("score_percent_rank").over(w_student_id_course_id_by_scor).alias("prev_score_percent_rank"),
        F.ntile(3).over(w_all_by_score_percent_rank).alias("score_percent_rank_tercile"),
        F.sum("score_dense_rank").over(w_score_dense_rank_term).alias("score_dense_rank_term_sum_score_dense_rank"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_score_dense_rank_term), F.lit(0)), 4).alias("score_share_of_score_dense_rank_term"),
    )
)

result = (
    windowed
    .select(
        "id",
        "enrolled_at",
        "next2_enrolled_at",
        "course_code",
        "prev_score_percent_rank",
        "score_percent_rank_tercile",
        "score_dense_rank_term_sum_score_dense_rank",
    )
    .orderBy(F.col("score_dense_rank_term_sum_score_dense_rank").asc())
)
