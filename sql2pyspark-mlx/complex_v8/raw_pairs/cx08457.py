from pyspark.sql import functions as F

base = (
    df_enrollments
    .filter((F.col("grade_status").isNotNull()) & (F.col("course_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.length(F.trim(F.col("term"))).alias("term_len"),
    )
)

grade_status_stats = (
    base
    .groupBy("grade_status")
    .agg(F.avg("term_len").alias("avg_term_len"))
)

outliers = (
    base.alias("s")
    .join(grade_status_stats.alias("g"), F.col("g.grade_status") == F.col("s.grade_status"), "inner")
    .filter(F.col("s.term_len") > F.col("g.avg_term_len"))
    .select(
        "s.*",
        "g.avg_term_len",
        F.round(F.col("s.term_len") - F.col("g.avg_term_len"), 2).alias("term_len_minus_avg"),
    )
)

joined_course = (
    outliers.alias("s")
    .join(df_courses.alias("c"), (F.col("c.id") == F.col("s.course_id")) & (F.col("c.level") == "intro"), "inner")
    .filter(F.col("c.dept") == "CS")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        "s.term_len",
        "s.term_len_minus_avg",
        "c.course_code",
        F.col("c.dept").alias("course_dept"),
    )
)

labeled = (
    joined_course
    .select(
        "id",
        "score",
        "grade_status",
        "enrolled_at",
        "term_len",
        "term_len_minus_avg",
        "course_dept",
        F.date_format(F.col("enrolled_at"), "yyyy-MM-dd").alias("enrolled_at_label"),
        F.coalesce(F.col("term_len_minus_avg"), F.lit(0.0)).alias("term_len_minus_avg_filled"),
    )
)

result = (
    labeled
    .groupingSets(
        [["grade_status", "term_len", "course_dept"], ["grade_status"], []],
        "grade_status", "term_len", "course_dept",
    )
    .agg(
        F.min("term_len_minus_avg").alias("min_term_len_minus_avg"),
        F.count("*").alias("row_count"),
        F.countDistinct("grade_status").alias("distinct_grade_status"),
    )
    .orderBy(
        F.col("grade_status").asc_nulls_last(),
        F.col("term_len").asc_nulls_last(),
        F.col("course_dept").asc_nulls_last(),
    )
)
