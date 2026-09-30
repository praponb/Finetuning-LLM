from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_score_ties = Window.orderBy(F.col("score").desc())
w_course_id = Window.partitionBy("course_id")

valid = (
    df_enrollments
    .filter(F.col("course_id") <= 8)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat(F.col("term"), F.lit("#"), F.col("id").cast("string")).alias("term_ref"),
        F.year(F.col("enrolled_at")).alias("enrolled_at_year"),
        F.concat_ws("-", F.col("grade_status"), F.col("term")).alias("grade_status_term_label"),
    )
)

sequenced = (
    valid
    .select(
        "id",
        "student_id",
        "term",
        "grade_status",
        "enrolled_at",
        "enrolled_at_year",
        "grade_status_term_label",
        F.round(F.percent_rank().over(w_all_by_score_ties), 3).alias("score_percent_rank"),
        F.max("enrolled_at_year").over(w_course_id).alias("course_id_max_enrolled_at_year"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_course_id), F.lit(0)), 4).alias("score_share_of_course_id"),
    )
)

derived = (
    sequenced
    .filter(~F.col("term").isin("2025T2", "2025T1"))
    .select(
        "*",
        F.when(F.col("enrolled_at_year") >= 2026, "A").when(F.col("enrolled_at_year") >= 2025, "B").otherwise("C").alias("enrolled_at_year_tier"),
        F.concat_ws("-", F.col("term"), F.col("grade_status_term_label")).alias("term_grade_status_term_label_label"),
        F.substring(F.trim(F.col("term")), 1, 2).alias("term_prefix"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.min("enrolled_at").alias("first_enrollment_enrolled_at"),
        F.min("enrolled_at").alias("first_enrollment_enrolled_at_2"),
    )
)

with_counts = (
    derived.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        "e.first_enrollment_enrolled_at",
        "e.first_enrollment_enrolled_at_2",
    )
)

labeled = (
    with_counts
    .filter(F.col("term_grade_status_term_label_label").isNotNull())
    .select(
        "*",
        F.date_trunc("year", F.col("enrolled_at")).alias("enrolled_at_year_2"),
        F.concat_ws("-", F.col("grade_status_term_label"), F.col("enrolled_at_year_tier")).alias("grade_status_term_label_enrolled_at_year_tier_la"),
    )
)

with_children_enrollments = (
    labeled.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.score").between(5.6, 78.8)), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.term",
        "s.enrolled_at",
        "s.enrolled_at_year",
        "s.grade_status_term_label",
        "s.score_percent_rank",
        "s.score_share_of_course_id",
        "s.first_enrollment_enrolled_at_2",
        "s.enrolled_at_year_2",
        "s.grade_status_term_label_enrolled_at_year_tier_la",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.course_id").alias("enrollment_course_id"),
    )
)

joined_enrollment = (
    with_children_enrollments.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.id") == F.col("s.enrollment_id"), "left")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("e.course_id"), "left")
    .select(
        "s.*",
        F.col("e.course_id").alias("enrollment_course_id_2"),
        F.col("c.level").alias("course_level"),
    )
)

result = (
    joined_enrollment
    .filter(F.col("score_share_of_course_id") >= 0.1)
    .select(
        "id",
        "student_id",
        "term",
        "enrolled_at",
        "enrolled_at_year",
        "grade_status_term_label",
        "score_percent_rank",
        "score_share_of_course_id",
        "first_enrollment_enrolled_at_2",
        "enrolled_at_year_2",
        "grade_status_term_label_enrolled_at_year_tier_la",
        "enrollment_id",
        "enrollment_term",
        "enrollment_course_id_2",
        "course_level",
        F.when(F.col("score_share_of_course_id") >= 0.6, "HIGH").when(F.col("score_share_of_course_id") >= 0.5, "MEDIUM").otherwise("LOW").alias("score_share_of_course_id_tier"),
        F.concat(F.col("term"), F.lit("#"), F.col("id").cast("string")).alias("term_ref"),
    )
    .orderBy(F.col("term_ref").desc_nulls_first())
)
