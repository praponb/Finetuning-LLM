from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("term").orderBy(F.col("days_since_enrolled_at").desc(), F.col("id").desc())
w_dedup_2 = Window.partitionBy("student_id", "enrollment_term").orderBy(F.col("enrollment_enrolled_at").desc(), F.col("id").desc(), F.col("enrollment_id").desc())

filtered_enrollments = (
    df_enrollments
    .filter((F.col("score") > 43.5) & (F.hour(F.col("enrolled_at")).between(7, 9)))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
        F.lower(F.regexp_replace(F.trim(F.col("grade_status")), " ", "_")).alias("grade_status_slug"),
    )
)

latest = (
    filtered_enrollments
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

screened = (
    latest.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left_semi")
)

course_id_stats = (
    screened
    .groupBy("course_id")
    .agg(F.percentile_approx("grade_status_len", 0.5).alias("median_grade_status_len"))
)

compared = (
    screened.alias("s")
    .join(course_id_stats.alias("g"), F.col("g.course_id") == F.col("s.course_id"), "inner")
    .filter(F.col("s.grade_status_len") >= F.col("g.median_grade_status_len"))
    .select(
        "s.*",
        "g.median_grade_status_len",
        F.round(F.col("s.grade_status_len") - F.col("g.median_grade_status_len"), 2).alias("grade_status_len_minus_median"),
    )
)

with_children_enrollments = (
    compared.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.days_since_enrolled_at",
        "s.grade_status_slug",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
    )
)

screened_2 = (
    with_children_enrollments.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.enrolled_at") >= F.lit("2025-01-31 09:00:00").cast("timestamp")) & (F.col("e.student_id").isNotNull()), "left_anti")
)

latest_per_key = (
    screened_2
    .withColumn("row_num", F.row_number().over(w_dedup_2))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest_per_key
    .select(
        "id",
        "student_id",
        "term",
        "score",
        "days_since_enrolled_at",
        "enrollment_id",
        "enrollment_grade_status",
        "enrollment_score",
    )
    .orderBy("term")
)
