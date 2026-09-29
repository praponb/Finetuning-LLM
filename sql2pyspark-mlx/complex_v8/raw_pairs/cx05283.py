from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("student_id", "term").orderBy(F.col("score_distance").desc(), F.col("id").desc())
w_grade_status_student_id_by_s = Window.partitionBy("grade_status", "student_id").orderBy("score_distance", "id")
w_student_id = Window.partitionBy("student_id")
w_grade_status_by_score = Window.partitionBy("grade_status").orderBy(F.col("score").desc(), "id")

recent_enrollments = (
    df_enrollments
    .filter(F.col("student_id") >= 4)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.abs(F.col("score") - 38.1).alias("score_distance"),
    )
)

deduped = (
    recent_enrollments
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

windowed = (
    deduped
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "score_distance",
        F.dense_rank().over(w_grade_status_student_id_by_s).alias("score_distance_dense_rank"),
        F.count(F.lit(1)).over(w_student_id).alias("rows_in_student_id"),
        (F.col("score") - F.lag("score").over(w_grade_status_by_score)).alias("score_change"),
        F.lag("score", 3).over(w_grade_status_by_score).alias("prev3_score"),
    )
)

result = (
    windowed
    .groupingSets(
        [["grade_status", "term"], ["grade_status"], ["term"], []],
        "grade_status", "term",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.max("rows_in_student_id").alias("max_rows_in_student_id"),
        F.array_sort(F.collect_set("grade_status")).alias("grade_status_list"),
        F.round(F.sum("score_distance"), 2).alias("total_score_distance"),
    )
    .orderBy(F.col("grade_status").asc_nulls_last(), F.col("term").asc_nulls_last())
)
