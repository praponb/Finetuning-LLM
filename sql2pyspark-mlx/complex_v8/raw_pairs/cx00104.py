from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("term").orderBy(F.col("term_len").desc(), F.col("id").desc())

base = (
    df_enrollments
    .filter((~F.col("grade_status").isin("FAIL", "WITHDRAWN")) & (F.col("enrolled_at") >= F.lit("2025-02-06 11:00:00").cast("timestamp")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat_ws(" / ", F.col("grade_status"), F.col("term")).alias("grade_status_term_label"),
        F.when(F.col("term").isin("2024T2", "2025T2"), "group_a").when(F.col("term") == "2024T1", "group_b").otherwise("other").alias("term_group"),
        F.length(F.trim(F.col("term"))).alias("term_len"),
    )
)

unique_rows = (
    base
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    unique_rows
    .rollup("student_id", "course_id", "term")
    .agg(
        F.grouping("student_id").alias("is_total_student_id"),
        F.round(F.sum("score"), 2).alias("total_score"),
    )
    .select(
        "student_id",
        "course_id",
        F.coalesce(F.col("term"), F.lit("ALL")).alias("term"),
        "is_total_student_id",
        "total_score",
    )
    .orderBy(
        F.col("student_id").asc_nulls_last(),
        F.col("course_id").asc_nulls_last(),
        F.col("term").asc_nulls_last(),
    )
)
