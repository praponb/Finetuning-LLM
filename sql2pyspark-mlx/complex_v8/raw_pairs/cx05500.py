from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_score = Window.orderBy(F.col("score").desc_nulls_first(), "id")
w_course_id_score_distance_tie = Window.partitionBy("course_id", "score_distance_tier").orderBy(F.col("score").desc(), "id")
w_course_id = Window.partitionBy("course_id")

cleaned_enrollments = (
    df_enrollments
    .filter(F.col("term") != "2024T1")
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.abs(F.col("score") - 50.9).alias("score_distance"),
    )
)

scored = (
    cleaned_enrollments
    .filter(F.col("course_id") <= 3)
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "score_distance",
        F.when(F.col("score_distance") >= 80.0, "HIGH").when(F.col("score_distance") >= 35.0, "MEDIUM").otherwise("LOW").alias("score_distance_tier"),
        F.substring(F.trim(F.col("term")), 1, 4).alias("term_prefix"),
        F.concat_ws(" / ", F.col("term"), F.col("grade_status")).alias("term_grade_status_label"),
    )
)

windowed = (
    scored
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "score_distance",
        "score_distance_tier",
        "term_prefix",
        F.dense_rank().over(w_all_by_score).alias("score_dense_rank"),
        F.lag("score_distance", 2).over(w_course_id_score_distance_tie).alias("prev2_score_distance"),
        F.row_number().over(w_all_by_score).alias("rn"),
        F.round(F.avg("score").over(w_course_id), 2).alias("course_id_avg_score"),
    )
)

result = (
    windowed
    .filter(F.col("course_id").isNotNull())
    .select(
        "id",
        "student_id",
        "score",
        "score_dense_rank",
        "prev2_score_distance",
        F.substring(F.trim(F.col("term_prefix")), 1, 3).alias("term_prefix_prefix"),
        F.abs(F.col("course_id_avg_score") - 45.0).alias("course_id_avg_score_distance"),
    )
    .orderBy("score", "id")
    .limit(5)
)
