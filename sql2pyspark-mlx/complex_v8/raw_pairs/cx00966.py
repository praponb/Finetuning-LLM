from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_score_tier_term = Window.partitionBy("score_tier", "term")
w_all_by_score_distance_filled = Window.orderBy("score_distance_filled", "id").rowsBetween(-2, -1)
w_all_by_student_gpa_moving_2 = Window.orderBy(F.col("student_gpa").desc(), "id").rowsBetween(-2, Window.currentRow)

filtered = (
    df_enrollments
    .filter(F.col("score") > 30.0)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
        F.when(F.col("score") >= 78.8, "HIGH").when(F.col("score") >= 43.5, "MEDIUM").otherwise("LOW").alias("score_tier"),
        F.abs(F.col("score") - 50.9).alias("score_distance"),
    )
)

labeled = (
    filtered
    .filter(F.col("grade_status").isin("INCOMPLETE", "PASS", "FAIL"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        "score_tier",
        "score_distance",
        F.date_format(F.col("enrolled_at"), "yyyy-MM-dd").alias("enrolled_at_label"),
    )
)

derived = (
    labeled
    .select(
        "id",
        "student_id",
        "term",
        "grade_status",
        "enrolled_at",
        "score_tier",
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
        F.coalesce(F.col("score_distance"), F.lit(0.0)).alias("score_distance_filled"),
        F.lower(F.regexp_replace(F.trim(F.col("score_tier")), " ", "_")).alias("score_tier_slug"),
    )
)

joined = (
    derived.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.term",
        "s.score_tier",
        "s.grade_status_or_unknown",
        "s.score_distance_filled",
        "s.score_tier_slug",
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        "st.student_name",
        F.col("st.gpa").alias("student_gpa"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(F.max("enrolled_at").alias("last_enrollment_enrolled_at"))
)

with_stats = (
    joined.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select("s.*", "e.last_enrollment_enrolled_at")
)

ranked = (
    with_stats
    .select(
        "*",
        F.round(F.col("score_distance_filled") - F.avg("score_distance_filled").over(w_score_tier_term), 2).alias("score_distance_filled_vs_score_tier_term_avg"),
        F.round(F.col("score_distance_filled") / F.nullif(F.sum("score_distance_filled").over(w_score_tier_term), F.lit(0)), 4).alias("score_distance_filled_share_of_score_tier_term"),
        F.round(F.avg("student_gpa").over(w_all_by_score_distance_filled), 2).alias("student_gpa_avg_2_prior"),
        F.round(F.avg("student_gpa").over(w_all_by_student_gpa_moving_2), 2).alias("student_gpa_avg_3"),
    )
)

enrollment_stats_2 = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.count("*").alias("enrollment_count"),
        F.min("score").alias("min_enrollment_score"),
    )
)

profiled = (
    ranked.alias("s")
    .join(enrollment_stats_2.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
        "e.min_enrollment_score",
    )
)

result = (
    profiled
    .select(
        "id",
        "term",
        "score_tier",
        "grade_status_or_unknown",
        "score_distance_filled",
        "score_tier_slug",
        "student_enrolled_on",
        "student_name",
        "last_enrollment_enrolled_at",
        "score_distance_filled_vs_score_tier_term_avg",
        "score_distance_filled_share_of_score_tier_term",
        "student_gpa_avg_2_prior",
        "student_gpa_avg_3",
        "enrollment_count",
        F.when(F.col("score_tier").isin("LOW", "MEDIUM"), "group_a").when(F.col("score_tier") == "HIGH", "group_b").otherwise("other").alias("score_tier_group"),
        F.coalesce(F.col("student_gpa_avg_2_prior"), F.lit(0.0)).alias("student_gpa_avg_2_prior_filled"),
    )
    .orderBy(F.col("score_tier_group").asc_nulls_last())
)
