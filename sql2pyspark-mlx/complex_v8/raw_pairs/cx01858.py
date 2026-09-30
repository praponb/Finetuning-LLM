from pyspark.sql import functions as F

prepared = (
    df_enrollments
    .filter(F.col("score") <= 57.6)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
    )
)

annotated = (
    prepared
    .filter(F.col("score") >= 10.1)
    .withColumn("score_bucket", F.floor(F.col("score") / 25) * 25)
)

result = (
    annotated
    .groupingSets(
        [["course_id", "student_id"], ["course_id"]],
        "course_id", "student_id",
    )
    .agg(
        F.round(F.sum("score") / F.nullif(F.sum("score_bucket"), F.lit(0)), 3).alias("score_to_score_bucket_ratio"),
        F.countDistinct("term").alias("distinct_terms"),
    )
    .orderBy(
        F.col("course_id").asc_nulls_last(),
        F.col("student_id").asc_nulls_last(),
    )
)
