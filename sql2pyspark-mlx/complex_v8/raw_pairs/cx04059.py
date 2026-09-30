from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("student_id").orderBy(F.col("score").desc(), F.col("id").desc())

recent = (
    df_enrollments
    .filter(F.col("course_id") <= 7)
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

deduped = (
    recent
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    deduped
    .cube("student_id", "term")
    .agg(
        F.array_sort(F.collect_set("grade_status")).alias("grade_status_list"),
        F.sum(F.when(F.col("grade_status") == "WITHDRAWN", 1).otherwise(0)).alias("n_grade_status_withdrawn"),
    )
    .orderBy(F.col("student_id").asc_nulls_last(), F.col("term").asc_nulls_last())
)
