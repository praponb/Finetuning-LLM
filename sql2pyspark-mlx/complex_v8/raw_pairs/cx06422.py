from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_term_len_2 = Window.orderBy("term_len_2", "id")
w_term_len_course_id = Window.partitionBy("term_len", "course_id")
w_all_by_term_len_share_of_ter = Window.orderBy("term_len_share_of_term_len_course_id", "id")
w_course_id_student_id = Window.partitionBy("course_id", "student_id")

filtered_enrollments = (
    df_enrollments
    .filter(F.col("score").between(13.9, 85.2))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.length(F.trim(F.col("term"))).alias("term_len"),
        F.length(F.trim(F.col("term"))).alias("term_len_2"),
        F.abs(F.col("score") - 44.4).alias("score_distance"),
    )
)

metrics = (
    filtered_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "term_len_2",
        "score_distance",
        F.ntile(10).over(w_all_by_term_len_2).alias("term_len_2_decile"),
        F.round(F.col("term_len") / F.nullif(F.sum("term_len").over(w_term_len_course_id), F.lit(0)), 4).alias("term_len_share_of_term_len_course_id"),
        F.min("term_len_2").over(w_term_len_course_id).alias("term_len_course_id_min_term_len_2"),
    )
)

ranked = (
    metrics
    .withColumn("prev_score_distance", F.lag("score_distance").over(w_all_by_term_len_share_of_ter))
    .withColumn("course_id_student_id_sum_term_len_2_decile", F.sum("term_len_2_decile").over(w_course_id_student_id))
)

result = (
    ranked
    .select(
        "id",
        "course_id",
        "score",
        "term_len_2",
        "term_len_share_of_term_len_course_id",
        "term_len_course_id_min_term_len_2",
        "prev_score_distance",
        "course_id_student_id_sum_term_len_2_decile",
    )
    .orderBy("term_len_course_id_min_term_len_2", "prev_score_distance", "id")
    .limit(10)
)
