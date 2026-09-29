from pyspark.sql import functions as F

base = (
    df_courses
    .filter((F.col("credits") >= 2) & (F.col("level").isNotNull()))
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        F.abs(F.col("credits") - 3).alias("credits_distance"),
        F.when(F.col("credits") >= 3, "A").when(F.col("credits") >= 2, "B").otherwise("C").alias("credits_tier"),
    )
)

detail_rows_enrollments = (
    base.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.id")) & (F.col("e.grade_status") == "WITHDRAWN"), "inner")
    .select(
        "s.id",
        "s.course_code",
        "s.dept",
        "s.credits",
        "s.credits_tier",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
    )
)

stats = (
    detail_rows_enrollments
    .groupBy("enrollment_id")
    .agg(
        F.sum("enrollment_score").alias("total_enrollment_score"),
        F.count(F.when(~F.col("dept").isin("Biz", "Math"), 1)).alias("n_dept_not_in_biz_math"),
        F.round(F.sum("enrollment_score"), 2).alias("total_enrollment_score_2"),
        F.round(F.avg("enrollment_score"), 2).alias("avg_enrollment_score"),
    )
)

combined_details = (
    stats.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.id") == F.col("s.enrollment_id"), "left")
    .select(
        "s.enrollment_id",
        "s.total_enrollment_score",
        "s.n_dept_not_in_biz_math",
        "s.total_enrollment_score_2",
        "s.avg_enrollment_score",
        F.col("e.course_id").alias("enrollment_course_id"),
    )
)

line_level_enrollments = (
    combined_details.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.enrollment_course_id"), "inner")
    .select(
        "s.enrollment_id",
        "s.total_enrollment_score",
        "s.total_enrollment_score_2",
        "s.avg_enrollment_score",
        "s.enrollment_course_id",
        F.col("e.id").alias("enrollment_id_2"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.score").alias("enrollment_score"),
    )
)

result = (
    line_level_enrollments
    .groupingSets(
        [["enrollment_id", "enrollment_course_id"], ["enrollment_id"], ["enrollment_course_id"], []],
        "enrollment_id", "enrollment_course_id",
    )
    .agg(
        F.array_sort(F.collect_set("enrollment_grade_status")).alias("enrollment_grade_status_list"),
        F.sum(F.when(F.col("enrollment_grade_status").isNotNull(), 1).otherwise(0)).alias("n_enrollment_grade_status_is_n"),
        F.countDistinct("enrollment_course_id").alias("distinct_enrollment_course_ids"),
    )
)
