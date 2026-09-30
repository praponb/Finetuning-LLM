from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("student_id", "course_id").orderBy(F.col("enrolled_at").desc(), F.col("id").desc())

prepared = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
    )
)

latest_per_key = (
    prepared
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest_per_key
    .groupingSets(
        [["course_id", "grade_status", "student_id"], ["course_id"], ["grade_status"], []],
        "course_id", "grade_status", "student_id",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.countDistinct("course_id").alias("distinct_course_ids"),
        F.sum(F.when(F.col("grade_status").isNotNull(), 1).otherwise(0)).alias("n_grade_status_is_not_null"),
        F.count("*").alias("cnt"),
    )
    .select(
        "course_id",
        F.coalesce(F.col("grade_status"), F.lit("ALL")).alias("grade_status"),
        "student_id",
        "gid",
        "distinct_course_ids",
        "n_grade_status_is_not_null",
        "cnt",
    )
    .orderBy(
        F.col("course_id").asc_nulls_last(),
        F.col("grade_status").asc_nulls_last(),
        F.col("student_id").asc_nulls_last(),
    )
)
