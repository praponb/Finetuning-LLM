from pyspark.sql import functions as F

prepared = (
    df_enrollments
    .filter((F.col("student_id").isNotNull()) & (F.col("course_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.lower(F.regexp_replace(F.trim(F.col("term")), " ", "_")).alias("term_slug"),
        F.length(F.trim(F.col("term"))).alias("term_len"),
    )
)

with_activity = (
    prepared
    .crossJoin(prepared.agg(F.avg("term_len").alias("overall_avg_term_len")))
    .filter(F.col("term_len") > F.col("overall_avg_term_len"))
    .drop("overall_avg_term_len")
)

summary = (
    with_activity
    .groupBy("term_len", "grade_status")
    .agg(
        F.array_sort(F.collect_set("grade_status")).alias("grade_status_list"),
        F.count(F.when(F.col("grade_status") == "WITHDRAWN", 1)).alias("n_grade_status_withdrawn"),
        F.count("*").alias("cnt"),
        F.round(F.sum("term_len") / F.nullif(F.sum("score"), F.lit(0)), 3).alias("term_len_to_score_ratio"),
        F.countDistinct("grade_status").alias("distinct_grade_status"),
    )
)

result = (
    summary
    .select(
        "term_len",
        "grade_status",
        "grade_status_list",
        "n_grade_status_withdrawn",
        "cnt",
        "term_len_to_score_ratio",
        "distinct_grade_status",
        F.concat(F.col("grade_status"), F.lit("#"), F.col("n_grade_status_withdrawn").cast("string")).alias("grade_status_ref"),
    )
    .orderBy(F.col("grade_status_ref").desc_nulls_first())
)
