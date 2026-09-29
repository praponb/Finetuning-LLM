from pyspark.sql import functions as F

cleaned_enrollments = (
    df_enrollments
    .filter((F.col("term") != "2025T2") & (F.col("grade_status").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.when(F.col("score") >= 81.4, "HIGH").when(F.col("score") >= 34.2, "MEDIUM").otherwise("LOW").alias("score_tier"),
    )
)

detail_rows = (
    cleaned_enrollments.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.enrolled_at",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
        F.col("e.score").alias("enrollment_score"),
    )
)

line_level = (
    detail_rows.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.term").isNotNull()), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.enrolled_at",
        "s.enrollment_id",
        "s.enrollment_enrolled_at",
        "s.enrollment_score",
        F.col("e.id").alias("enrollment_id_2"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.score").alias("enrollment_score_2"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.student_id").alias("enrollment_student_id"),
    )
)

student_id_stats = (
    line_level
    .groupBy("student_id")
    .agg(F.avg("score").alias("avg_score"))
)

vs_group = (
    line_level.alias("s")
    .join(student_id_stats.alias("g"), F.col("g.student_id") == F.col("s.student_id"), "inner")
    .filter(F.col("s.score") > F.col("g.avg_score"))
    .select(
        "s.*",
        "g.avg_score",
        F.round(F.col("s.score") - F.col("g.avg_score"), 2).alias("score_minus_avg"),
    )
)

summary = (
    vs_group
    .filter(F.col("enrollment_grade_status") == "INCOMPLETE")
    .groupBy("student_id", F.year("enrolled_at").alias("enrolled_at_year"))
    .agg(
        F.round(F.stddev("score_minus_avg"), 2).alias("stddev_score_minus_avg"),
        F.round(F.avg("avg_score"), 2).alias("avg_avg_score"),
        F.array_sort(F.collect_set("term")).alias("term_list"),
        F.sum("score").alias("sum_score_check"),
    )
    .filter(F.col("sum_score_check") > 26.2)
    .drop("sum_score_check")
)

qualified = (
    summary.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.term").isNotNull()) & (F.col("e.enrolled_at") < F.lit("2025-02-12 09:00:00").cast("timestamp")), "left_anti")
)

with_details = (
    qualified.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select("s.*", F.col("st.major").alias("student_major"), "st.student_name")
)

result = (
    with_details
    .groupBy("enrolled_at_year")
    .pivot("student_major", ["Art", "Biz"])
    .agg(F.sum("stddev_score_minus_avg"))
)
