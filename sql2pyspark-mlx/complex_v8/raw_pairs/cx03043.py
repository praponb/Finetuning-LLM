from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("course_id").orderBy(F.col("enrolled_at").desc(), F.col("id").desc())

cleaned_enrollments = (
    df_enrollments
    .filter((F.col("grade_status").isin("FAIL", "WITHDRAWN")) & (F.col("score") < 74.9))
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.when(F.col("score") >= 66.0, "large").when(F.col("score") >= 34.2, "medium").otherwise("small").alias("score_tier"),
    )
)

latest_per_key = (
    cleaned_enrollments
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest_per_key
    .rollup("grade_status", "score_tier")
    .agg(
        F.sum(F.when(F.col("grade_status").isin("FAIL", "INCOMPLETE"), F.col("score"))).alias("score_grade_status_in_fail_incompl"),
        F.min("score").alias("min_score"),
    )
    .orderBy(
        F.col("grade_status").asc_nulls_last(),
        F.col("score_tier").asc_nulls_last(),
    )
)
