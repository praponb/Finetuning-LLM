from pyspark.sql import functions as F

prepared_enrollments = (
    df_enrollments
    .filter(F.col("score") < 87.8)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.date_format(F.col("enrolled_at"), "MMM yyyy").alias("enrolled_at_label"),
    )
)

summary = (
    prepared_enrollments
    .groupBy("term", "student_id", "course_id")
    .agg(
        F.min("enrolled_at").alias("first_enrolled_at"),
        F.max("enrolled_at").alias("last_enrolled_at"),
    )
)

result = (
    summary
    .cube("course_id", "term")
    .agg(
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("term") != "2024T1", 1).otherwise(0)).alias("n_term_2024t1"),
        F.max("first_enrolled_at").alias("last_first_enrolled_at"),
    )
    .select(
        "course_id",
        F.coalesce(F.col("term"), F.lit("ALL")).alias("term"),
        "row_count",
        "n_term_2024t1",
        "last_first_enrolled_at",
    )
    .orderBy(F.col("course_id").asc_nulls_last(), F.col("term").asc_nulls_last())
)
