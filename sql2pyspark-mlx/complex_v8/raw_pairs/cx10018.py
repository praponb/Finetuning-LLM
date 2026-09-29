from pyspark.sql import functions as F

valid_enrollments = (
    df_enrollments
    .filter((F.col("course_id").isNotNull()) & (F.col("score") >= 10.1))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.dayofweek(F.col("enrolled_at")).alias("enrolled_at_dayofweek"),
        F.concat_ws(" / ", F.col("grade_status"), F.col("term")).alias("grade_status_term_label"),
        F.concat_ws("|", F.col("grade_status"), F.col("term")).alias("grade_status_term_label_2"),
    )
)

without_matches = (
    valid_enrollments.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.grade_status") == "WITHDRAWN"), "left_anti")
)

derived = (
    without_matches
    .select(
        "id",
        "student_id",
        "course_id",
        "enrolled_at_dayofweek",
        "grade_status_term_label",
        "grade_status_term_label_2",
        F.dayofweek(F.col("enrolled_at")).alias("enrolled_at_dayofweek_2"),
        F.dayofweek(F.col("enrolled_at")).alias("enrolled_at_dayofweek_3"),
        F.to_date(F.col("enrolled_at")).alias("enrolled_at_date"),
    )
)

exploded_detail = (
    derived.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.enrolled_at_dayofweek_2",
        "s.enrolled_at_dayofweek_3",
        "s.enrolled_at_date",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.student_id").alias("enrollment_student_id"),
    )
)

enrollment_term_stats = (
    exploded_detail
    .groupBy("enrollment_term")
    .agg(F.percentile_approx("enrolled_at_dayofweek_3", 0.5).alias("median_enrolled_at_dayofweek_3"))
)

outliers = (
    exploded_detail.alias("s")
    .join(enrollment_term_stats.alias("g"), F.col("g.enrollment_term") == F.col("s.enrollment_term"), "inner")
    .filter(F.col("s.enrolled_at_dayofweek_3") >= F.col("g.median_enrolled_at_dayofweek_3"))
    .select(
        "s.*",
        "g.median_enrolled_at_dayofweek_3",
        F.round(F.col("s.enrolled_at_dayofweek_3") - F.col("g.median_enrolled_at_dayofweek_3"), 2).alias("enrolled_at_dayofweek_3_minus_median"),
    )
)

result = (
    outliers
    .select(
        "id",
        "student_id",
        "course_id",
        "enrolled_at_dayofweek_2",
        "enrolled_at_dayofweek_3",
        "enrolled_at_date",
        "enrollment_id",
        "enrollment_term",
        "enrollment_student_id",
        "enrolled_at_dayofweek_3_minus_median",
        F.length(F.trim(F.col("enrollment_term"))).alias("enrollment_term_len"),
    )
    .orderBy(F.col("enrolled_at_dayofweek_3_minus_median").desc())
)
