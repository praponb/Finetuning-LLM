from pyspark.sql import functions as F

base_enrollments = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.when(F.col("grade_status").isin("WITHDRAWN", "PASS"), "group_a").when(F.col("grade_status") == "INCOMPLETE", "group_b").otherwise("other").alias("grade_status_group"),
        F.concat(F.col("grade_status"), F.lit("#"), F.col("course_id").cast("string")).alias("grade_status_ref"),
        F.length(F.trim(F.col("term"))).alias("term_len"),
    )
)

grade_status_stats = (
    base_enrollments
    .groupBy("grade_status")
    .agg(F.max("term_len").alias("max_term_len"))
)

compared = (
    base_enrollments.alias("s")
    .join(grade_status_stats.alias("g"), F.col("g.grade_status") == F.col("s.grade_status"), "inner")
    .filter(F.col("s.term_len") == F.col("g.max_term_len"))
    .select(
        "s.*",
        "g.max_term_len",
        F.round(F.col("s.term_len") - F.col("g.max_term_len"), 2).alias("term_len_minus_max"),
    )
)

result = (
    compared
    .cube("grade_status", "term_len", "student_id")
    .agg(
        F.max("max_term_len").alias("max_max_term_len"),
        F.sum("term_len").alias("total_term_len"),
    )
)
