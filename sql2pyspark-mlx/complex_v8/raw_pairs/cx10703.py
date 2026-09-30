from pyspark.sql import functions as F

recent_enrollments = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.round(F.col("score") * 1.07, 2).alias("score_adj"),
        F.when(F.col("score") >= 83.1, "A").when(F.col("score") >= 34.2, "B").otherwise("C").alias("score_tier"),
        F.initcap(F.trim(F.col("grade_status"))).alias("grade_status_clean"),
    )
)

scored = (
    recent_enrollments
    .filter(F.upper(F.trim(F.col("grade_status_clean"))).like("F%"))
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "score_adj",
        "grade_status_clean",
        F.date_format(F.col("enrolled_at"), "yyyy-MM").alias("enrolled_at_label"),
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
        F.year(F.col("enrolled_at")).alias("enrolled_at_year"),
    )
)

filtered = (
    scored.alias("s")
    .join(df_courses.alias("c"), (F.col("c.id") == F.col("s.course_id")) & (F.col("c.level").isin("advanced", "core")) & (F.col("c.credits") > 2), "left_semi")
)

tagged = (
    filtered
    .filter(F.col("score_adj") > 22.5)
    .withColumn("score_per_enrolled_at_year", F.round(F.col("score") / F.nullif(F.col("enrolled_at_year"), F.lit(0)), 3))
)

result = (
    tagged
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "score_adj",
        "grade_status_clean",
        "enrolled_at_label",
        "score_filled",
        "enrolled_at_year",
        F.round(F.col("score") * 1.07, 2).alias("score_adj_2"),
        F.lower(F.trim(F.col("enrolled_at_label"))).alias("enrolled_at_label_clean"),
    )
    .orderBy(F.col("student_id").desc())
)
