from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_credits_tier_enrollment_enro = Window.partitionBy("credits_tier", F.to_date("enrollment_enrolled_at")).orderBy("enrollment_score", "id", "enrollment_id")
w_enrollment_term_enrollment_g = Window.partitionBy("enrollment_term", "enrollment_grade_status")

prepared_courses = (
    df_courses
    .filter(F.col("level").isin("core", "intro"))
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
        F.coalesce(F.col("credits"), F.lit(0)).alias("credits_filled"),
        F.when(F.col("credits") >= 3, "A").when(F.col("credits") >= 2, "B").otherwise("C").alias("credits_tier"),
    )
)

detail_rows = (
    prepared_courses.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.dept",
        "s.credits",
        "s.level",
        "s.credits_filled",
        "s.credits_tier",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
    )
)

windowed = (
    detail_rows
    .select(
        "id",
        "dept",
        "credits",
        "level",
        "credits_filled",
        "credits_tier",
        "enrollment_id",
        "enrollment_term",
        "enrollment_grade_status",
        "enrollment_enrolled_at",
        (F.col("enrollment_score") - F.lag("enrollment_score").over(w_credits_tier_enrollment_enro)).alias("enrollment_score_change"),
        F.size(F.collect_set("level").over(w_enrollment_term_enrollment_g)).alias("distinct_level_in_enrollment_term_enrollment_gra"),
    )
)

detail_rows_enrollments = (
    windowed.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.dept",
        "s.credits",
        "s.level",
        "s.credits_filled",
        "s.credits_tier",
        "s.enrollment_id",
        "s.enrollment_enrolled_at",
        "s.enrollment_score_change",
        "s.distinct_level_in_enrollment_term_enrollment_gra",
        F.col("e.id").alias("enrollment_id_2"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at_2"),
        F.col("e.grade_status").alias("enrollment_grade_status_2"),
        F.col("e.term").alias("enrollment_term_2"),
    )
)

scored = (
    detail_rows_enrollments
    .filter(F.col("enrollment_id_2") >= 51)
    .select(
        "id",
        "dept",
        "credits",
        "credits_filled",
        "credits_tier",
        "enrollment_id",
        "enrollment_enrolled_at",
        "enrollment_id_2",
        "enrollment_score",
        "enrollment_term_2",
        F.substring(F.trim(F.col("level")), 1, 4).alias("level_prefix"),
        F.abs(F.col("enrollment_score") - 49.0).alias("enrollment_score_distance"),
        F.when(F.col("enrollment_score") >= 77.0, "HIGH").when(F.col("enrollment_score") >= 20.6, "MEDIUM").otherwise("LOW").alias("enrollment_score_tier"),
    )
)

result = (
    scored
    .select(
        "id",
        "dept",
        "credits_filled",
        "credits_tier",
        "enrollment_id",
        "enrollment_enrolled_at",
        "enrollment_id_2",
        "enrollment_score",
        "enrollment_score_tier",
    )
)
