from pyspark.sql import functions as F

base = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.when(F.col("score") >= 58.8, "A").when(F.col("score") >= 35.8, "B").otherwise("C").alias("score_tier"),
    )
)

exploded_detail = (
    base.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.month(F.col("e.enrolled_at")).isin(2, 3)), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status",
        "s.enrolled_at",
        "s.score_tier",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.student_id").alias("enrollment_student_id"),
    )
)

result = (
    exploded_detail
    .groupingSets(
        [["enrollment_student_id", "enrollment_id"], ["enrollment_student_id"], []],
        "enrollment_student_id", "enrollment_id",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.round(F.sum("enrollment_score"), 2).alias("total_enrollment_score"),
        F.array_sort(F.collect_set("grade_status")).alias("grade_status_list"),
    )
    .orderBy(
        F.col("enrollment_student_id").asc_nulls_last(),
        F.col("enrollment_id").asc_nulls_last(),
    )
)
