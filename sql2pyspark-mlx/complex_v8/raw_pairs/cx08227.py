from pyspark.sql import functions as F

filtered = (
    df_enrollments
    .filter(F.col("term") != "2025T2")
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.year(F.col("enrolled_at")).alias("enrolled_at_year"),
        F.lower(F.regexp_replace(F.trim(F.col("grade_status")), " ", "_")).alias("grade_status_slug"),
    )
)

labeled = (
    filtered
    .withColumn("term_group", F.when(F.col("term").isin("2024T1", "2024T2"), "group_a").when(F.col("term") == "2025T2", "group_b").otherwise("other"))
)

result = (
    labeled
    .rollup("grade_status", "student_id")
    .agg(
        F.grouping_id().alias("gid"),
        F.sum(F.when(F.col("term") != "2024T1", F.col("enrolled_at_year"))).alias("enrolled_at_year_term_2024t1"),
        F.count("*").alias("cnt"),
    )
    .orderBy(
        F.col("grade_status").asc_nulls_last(),
        F.col("student_id").asc_nulls_last(),
    )
)
