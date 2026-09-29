from pyspark.sql import functions as F

recent_enrollments = (
    df_enrollments
    .filter((F.hour(F.col("enrolled_at")).between(9, 14)) | (F.col("student_id") <= 11))
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat(F.col("term"), F.lit("#"), F.col("student_id").cast("string")).alias("term_ref"),
        F.date_format(F.col("enrolled_at"), "MMM yyyy").alias("enrolled_at_label"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
    )
)

grade_status_stats = (
    recent_enrollments
    .groupBy("grade_status")
    .agg(F.avg("score").alias("avg_score"))
)

vs_group = (
    recent_enrollments.alias("s")
    .join(grade_status_stats.alias("g"), F.col("g.grade_status") == F.col("s.grade_status"), "inner")
    .filter(F.col("s.score") > F.col("g.avg_score"))
    .select(
        "s.*",
        "g.avg_score",
        F.round(F.col("s.score") - F.col("g.avg_score"), 2).alias("score_minus_avg"),
    )
)

enriched = (
    vs_group.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select("s.*", "st.student_name")
)

scored = (
    enriched
    .filter(F.col("course_id") >= 3)
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("student_name")), " ", "_")).alias("student_name_slug"),
        F.date_format(F.col("enrolled_at"), "yyyy-MM").alias("enrolled_at_label_2"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.count(F.when(F.col("term") != "2025T1", 1)).alias("enrollment_term_count"),
    )
)

summarized_rows = (
    scored.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_term_count"), F.lit(0)).alias("enrollment_term_count"),
    )
)

totals = (
    summarized_rows
    .groupBy("grade_status", "student_id", "course_id")
    .agg(
        F.round(F.sum("avg_score"), 2).alias("total_avg_score"),
        F.round(F.avg("days_since_enrolled_at"), 2).alias("avg_days_since_enrolled_at"),
        F.round(100 * F.count(F.when(F.col("grade_status") == "INCOMPLETE", 1)) / F.count("*"), 1).alias("pct_grade_status_incomplete"),
    )
)

derived = (
    totals
    .select(
        "*",
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
        F.coalesce(F.col("total_avg_score"), F.lit(0.0)).alias("total_avg_score_filled"),
    )
)

result = (
    derived
    .select(
        "grade_status",
        "student_id",
        "course_id",
        "avg_days_since_enrolled_at",
        "grade_status_len",
    )
    .orderBy(F.col("student_id").asc())
)
