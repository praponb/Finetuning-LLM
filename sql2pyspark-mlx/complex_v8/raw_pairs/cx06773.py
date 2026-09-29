from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_score = Window.orderBy("score", "id")
w_enrolled_at_to_date = Window.partitionBy(F.to_date("enrolled_at"))

cleaned_enrollments = (
    df_enrollments
    .filter((F.col("student_id").isNotNull()) & (F.col("enrolled_at") >= F.lit("2025-01-31 09:00:00").cast("timestamp")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.abs(F.col("score") - 38.5).alias("score_distance"),
        F.lower(F.regexp_replace(F.trim(F.col("term")), " ", "_")).alias("term_slug"),
        F.concat_ws(" / ", F.col("grade_status"), F.col("term")).alias("grade_status_term_label"),
    )
)

screened = (
    cleaned_enrollments.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.month(F.col("e.enrolled_at")).isin(1, 2)) & (~F.col("e.term").isin("2025T1", "2025T2")), "left_anti")
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.min("score").alias("min_enrollment_score"),
        F.count(F.when(F.col("grade_status") != "FAIL", 1)).alias("enrollment_grade_status_count"),
        F.count(F.when(F.col("term").isin("2024T2", "2025T1"), 1)).alias("enrollment_term_count"),
    )
)

with_counts = (
    screened.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        "e.min_enrollment_score",
        F.coalesce(F.col("e.enrollment_grade_status_count"), F.lit(0)).alias("enrollment_grade_status_count"),
        F.coalesce(F.col("e.enrollment_term_count"), F.lit(0)).alias("enrollment_term_count"),
    )
)

with_details_course = (
    with_counts.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        "s.min_enrollment_score",
        "s.enrollment_grade_status_count",
        "s.enrollment_term_count",
        F.col("c.level").alias("course_level"),
        F.col("c.credits").alias("course_credits"),
        "c.course_code",
    )
)

grade_status_stats = (
    with_details_course
    .groupBy("grade_status")
    .agg(F.avg("min_enrollment_score").alias("avg_min_enrollment_score"))
)

vs_group = (
    with_details_course.alias("s")
    .join(grade_status_stats.alias("g"), F.col("g.grade_status") == F.col("s.grade_status"), "inner")
    .filter(F.col("s.min_enrollment_score") > F.col("g.avg_min_enrollment_score"))
    .select(
        "s.*",
        "g.avg_min_enrollment_score",
        F.round(F.col("s.min_enrollment_score") - F.col("g.avg_min_enrollment_score"), 2).alias("min_enrollment_score_minus_avg"),
    )
)

screened_2 = (
    vs_group.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.student_id") <= 8), "left_anti")
)

sequenced = (
    screened_2
    .select(
        "*",
        F.row_number().over(w_all_by_score).alias("rn"),
        F.lead("min_enrollment_score").over(w_all_by_score).alias("next_min_enrollment_score"),
        F.lag("enrolled_at").over(w_all_by_score).alias("prev_enrolled_at"),
        F.count(F.lit(1)).over(w_enrolled_at_to_date).alias("rows_in_enrolled_at_to_date"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "min_enrollment_score",
        "enrollment_grade_status_count",
        "enrollment_term_count",
        "course_level",
        "course_code",
        "rn",
        "next_min_enrollment_score",
        "rows_in_enrolled_at_to_date",
    )
    .orderBy("score", "grade_status")
)
