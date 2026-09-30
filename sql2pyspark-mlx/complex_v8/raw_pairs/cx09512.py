from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("student_id").orderBy(F.col("enrolled_at").desc(), F.col("id").desc())
w_term_course_id = Window.partitionBy("term", "course_id")
w_term_by_score_full = Window.partitionBy("term").orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_term_by_score_ties = Window.partitionBy("term").orderBy("score")

prepared_enrollments = (
    df_enrollments
    .filter(F.col("enrolled_at") >= F.lit("2025-01-31 13:00:00").cast("timestamp"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat(F.col("term"), F.lit("#"), F.col("course_id").cast("string")).alias("term_ref"),
    )
)

filtered = (
    prepared_enrollments.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left_semi")
    .filter(F.col("term") == "2024T2")
)

latest = (
    filtered
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

ranked = (
    latest
    .select(
        "*",
        F.size(F.collect_set("student_id").over(w_term_course_id)).alias("distinct_student_id_in_term_course_id"),
        F.nth_value("enrolled_at", 2).over(w_term_by_score_full).alias("second_enrolled_at"),
        F.dense_rank().over(w_term_by_score_ties).alias("score_dense_rank"),
    )
)

result = (
    ranked
    .groupingSets(
        [["student_id", "distinct_student_id_in_term_course_id"], ["student_id"], []],
        "student_id", "distinct_student_id_in_term_course_id",
    )
    .agg(
        F.min("second_enrolled_at").alias("first_second_enrolled_at"),
        F.array_sort(F.collect_set("grade_status")).alias("grade_status_list"),
        F.max("distinct_student_id_in_term_course_id").alias("max_distinct_student_id_in_term_course_id"),
    )
)
