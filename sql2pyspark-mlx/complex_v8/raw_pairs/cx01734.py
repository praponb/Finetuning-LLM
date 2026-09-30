from pyspark.sql import functions as F

scoped_enrollments = (
    df_enrollments
    .filter((F.hour(F.col("enrolled_at")).between(9, 14)) | (F.col("score") < 87.8))
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.substring(F.trim(F.col("grade_status")), 1, 4).alias("grade_status_prefix"),
    )
)

qualified = (
    scoped_enrollments.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.score") >= 35.8), "left_semi")
)

totals = (
    qualified
    .filter(F.col("grade_status_prefix").isNotNull())
    .groupBy(
        F.date_trunc("month", "enrolled_at").alias("enrolled_at_month"),
        "course_id",
        "grade_status",
    )
    .agg(
        F.array_sort(F.collect_set("grade_status")).alias("grade_status_list"),
        F.sum("score").alias("total_score"),
        F.count("*").alias("cnt"),
    )
)

result = (
    totals
    .select(
        "enrolled_at_month",
        "grade_status",
        "grade_status_list",
        "total_score",
        "cnt",
        F.when(F.col("grade_status").isin("PASS", "WITHDRAWN"), "group_a").when(F.col("grade_status") == "FAIL", "group_b").otherwise("other").alias("grade_status_group"),
    )
    .orderBy("cnt", "enrolled_at_month")
)
