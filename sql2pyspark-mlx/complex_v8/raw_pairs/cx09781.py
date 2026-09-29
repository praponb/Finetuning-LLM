from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("course_id").orderBy(F.col("grade_status_len").desc(), F.col("id").desc())

cleaned = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        (F.col("enrolled_at") + F.expr("INTERVAL 30 MINUTES")).alias("enrolled_at_plus_30m"),
        F.initcap(F.trim(F.col("term"))).alias("term_clean"),
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
    )
)

unique_rows = (
    cleaned
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

derived = (
    unique_rows
    .select(
        "*",
        F.length(F.trim(F.col("term_clean"))).alias("term_clean_len"),
        F.hour(F.col("enrolled_at")).alias("enrolled_at_hour"),
        F.abs(F.col("grade_status_len") - 7).alias("grade_status_len_distance"),
    )
)

result = (
    derived
    .rollup("grade_status", "student_id", "term")
    .agg(
        F.grouping("grade_status").alias("is_total_grade_status"),
        F.sum("enrolled_at_hour").alias("total_enrolled_at_hour"),
        F.array_sort(F.collect_set("term")).alias("term_list"),
    )
    .orderBy(
        F.col("grade_status").asc_nulls_last(),
        F.col("student_id").asc_nulls_last(),
        F.col("term").asc_nulls_last(),
    )
)
