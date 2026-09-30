from pyspark.sql import functions as F

filtered_enrollments = (
    df_enrollments
    .filter((F.col("score") <= 58.8) & (F.col("course_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
    )
)

enriched_course = (
    filtered_enrollments.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select("s.*", F.col("c.dept").alias("course_dept"), "c.course_code")
)

enriched = (
    enriched_course
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        "score_filled",
        "course_dept",
        "course_code",
        F.date_trunc("hour", F.col("enrolled_at")).alias("enrolled_at_hour"),
        F.date_format(F.col("enrolled_at"), "yyyy-MM").alias("enrolled_at_label"),
    )
)

enriched_2 = (
    enriched
    .filter(F.col("score_filled") >= 35.0)
    .select(
        "*",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
        F.date_format(F.col("enrolled_at_hour"), "yyyy-MM-dd").alias("enrolled_at_hour_label"),
    )
)

result = (
    enriched_2
    .groupBy("grade_status")
    .pivot("course_dept", ["CS", "Art", "Bio", "Math"])
    .agg(F.sum("score_filled").alias("amount"), F.max("score_filled").alias("max_v"))
    .orderBy("grade_status")
)
