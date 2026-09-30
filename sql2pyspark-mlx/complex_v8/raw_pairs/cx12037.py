from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("term").orderBy("score", "id")

scoped = (
    df_enrollments
    .filter(F.col("student_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

latest = (
    scoped
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest
    .cube("course_id", "grade_status")
    .agg(
        F.max("enrolled_at").alias("last_enrolled_at"),
        F.sum(F.when(F.col("grade_status") == "INCOMPLETE", F.col("score"))).alias("score_grade_status_incomplete"),
    )
    .orderBy(
        F.col("course_id").asc_nulls_last(),
        F.col("grade_status").asc_nulls_last(),
    )
)
