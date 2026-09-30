from pyspark.sql import functions as F

cleaned = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.initcap(F.trim(F.col("term"))).alias("term_clean"),
        F.when(F.col("score") >= 80.2, "A").when(F.col("score") >= 36.6, "B").otherwise("C").alias("score_tier"),
        F.concat_ws("|", F.col("term"), F.col("grade_status")).alias("term_grade_status_label"),
    )
)

per_group = (
    cleaned
    .filter(F.col("course_id") <= 2)
    .groupBy("student_id")
    .agg(
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("term").isNotNull(), 1)).alias("n_term_is_not_null"),
        F.array_sort(F.collect_set("term")).alias("term_list"),
        F.round(100 * F.count(F.when(F.col("term").isin("2024T1", "2025T2"), 1)) / F.count("*"), 1).alias("pct_term_in_2024t1_2025t2"),
    )
)

scored = (
    per_group
    .select(
        "student_id",
        "term_list",
        F.array_join(F.array_sort(F.col("term_list")), ",").alias("term_list_text"),
    )
)

enriched = (
    scored
    .filter(F.col("term_list_text").isNotNull())
    .select(
        "student_id",
        "term_list",
        "term_list_text",
        F.lower(F.regexp_replace(F.trim(F.col("term_list_text")), " ", "_")).alias("term_list_text_slug"),
    )
)

result = (
    enriched
    .filter(F.size(F.col("term_list")) >= 1)
    .select("student_id", "term_list_text", "term_list_text_slug")
    .orderBy(F.col("term_list_text").asc(), F.col("term_list_text_slug").asc())
)
