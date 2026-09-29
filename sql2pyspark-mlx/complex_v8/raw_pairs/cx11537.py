from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id_course_id = Window.partitionBy("student_id", "course_id")
w_grade_status_course_id_by_sc = Window.partitionBy("grade_status", "course_id").orderBy("score", "id")
w_latest = Window.partitionBy("student_id").orderBy(F.col("distinct_grade_status_in_student_id_course_id").desc(), F.col("id").desc())

cleaned = (
    df_enrollments
    .filter(F.col("term") == "2025T1")
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
        F.lower(F.regexp_replace(F.trim(F.col("grade_status")), " ", "_")).alias("grade_status_slug"),
    )
)

ranked = (
    cleaned
    .select(
        "*",
        F.size(F.collect_set("grade_status").over(w_student_id_course_id)).alias("distinct_grade_status_in_student_id_course_id"),
        F.round(F.cume_dist().over(w_grade_status_course_id_by_sc), 3).alias("score_cume_dist"),
    )
)

latest = (
    ranked
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest
    .select(
        "id",
        "student_id",
        "score",
        "enrolled_at",
        "score_filled",
        "grade_status_slug",
        "distinct_grade_status_in_student_id_course_id",
        "score_cume_dist",
    )
    .orderBy(F.col("score").desc())
)
