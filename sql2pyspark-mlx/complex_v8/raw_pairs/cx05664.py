from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("grade_status").orderBy("score", "id")

recent = (
    df_enrollments
    .filter((F.month(F.col("enrolled_at")).isin(2, 3)) & (F.col("student_id") <= 14))
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.upper(F.trim(F.col("grade_status"))).alias("grade_status_clean"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.min("enrolled_at").alias("first_enrollment_enrolled_at"),
        F.count("*").alias("enrollment_count"),
    )
)

profiled = (
    recent.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        "e.first_enrollment_enrolled_at",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

latest = (
    profiled
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

combined_details_course = (
    latest.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status",
        "s.enrolled_at",
        "s.first_enrollment_enrolled_at",
        "s.enrollment_count",
        F.col("c.credits").alias("course_credits"),
    )
)

labeled = (
    combined_details_course
    .filter(F.hour(F.col("first_enrollment_enrolled_at")).between(7, 12))
    .withColumn("grade_status_slug", F.lower(F.regexp_replace(F.trim(F.col("grade_status")), " ", "_")))
)

filtered = (
    labeled.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.score").between(13.9, 66.0)), "left_semi")
)

scored = (
    filtered
    .select(
        "*",
        F.coalesce(F.col("enrollment_count"), F.lit(0)).alias("enrollment_count_filled"),
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
        F.concat_ws("-", F.col("grade_status_slug"), F.col("grade_status")).alias("grade_status_slug_grade_status_label"),
    )
)

result = (
    scored
    .groupingSets(
        [["grade_status_or_unknown", "grade_status", "enrollment_count_filled"], ["grade_status_or_unknown"], []],
        "grade_status_or_unknown", "grade_status", "enrollment_count_filled",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.count("*").alias("cnt"),
        F.round(F.sum("course_credits") / F.nullif(F.sum("enrollment_count_filled"), F.lit(0)), 3).alias("course_credits_to_enrollment_count_filled_ratio"),
    )
    .orderBy(
        F.col("grade_status_or_unknown").asc_nulls_last(),
        F.col("grade_status").asc_nulls_last(),
        F.col("enrollment_count_filled").asc_nulls_last(),
    )
)
