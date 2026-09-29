from pyspark.sql import functions as F

scoped = (
    df_enrollments
    .filter((F.col("course_id").isNotNull()) & (F.col("score") < 76.8))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.date_trunc("week", F.col("enrolled_at")).alias("enrolled_at_week"),
        F.dayofweek(F.col("enrolled_at")).alias("enrolled_at_dayofweek"),
    )
)

exploded_detail = (
    scoped.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.enrolled_at_dayofweek",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.course_id").alias("enrollment_course_id"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
        F.col("e.score").alias("enrollment_score"),
    )
)

per_group = (
    exploded_detail
    .filter(F.col("enrollment_course_id").isNotNull())
    .groupBy("term")
    .agg(
        F.array_sort(F.collect_set("term")).alias("term_list"),
        F.count("*").alias("row_count"),
        F.round(100 * F.count(F.when(F.col("enrollment_grade_status").isNotNull(), 1)) / F.count("*"), 1).alias("pct_enrollment_grade_status_is_n"),
        F.countDistinct("enrollment_course_id").alias("distinct_enrollment_course_ids"),
        F.countDistinct("term").alias("distinct_terms"),
        F.max("enrollment_score").alias("max_enrollment_score"),
    )
)

filtered = (
    per_group
    .crossJoin(per_group.agg(F.avg("distinct_enrollment_course_ids").alias("overall_avg_distinct_enrollment_course_ids")))
    .filter(F.col("distinct_enrollment_course_ids") > F.col("overall_avg_distinct_enrollment_course_ids"))
    .drop("overall_avg_distinct_enrollment_course_ids")
)

result = (
    filtered
    .select(
        "term",
        "term_list",
        "row_count",
        "pct_enrollment_grade_status_is_n",
        "distinct_enrollment_course_ids",
        "distinct_terms",
        "max_enrollment_score",
    )
    .orderBy("row_count", F.col("max_enrollment_score").desc())
)
