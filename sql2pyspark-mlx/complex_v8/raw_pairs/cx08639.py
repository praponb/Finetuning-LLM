from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_n_enrollment_term_is_not_nul = Window.partitionBy("n_enrollment_term_is_not_null", "term").orderBy("median_enrollment_score", "term")
w_term = Window.partitionBy("term")
w_term_by_median_enrollment_sc = Window.partitionBy("term").orderBy(F.col("median_enrollment_score").desc(), "term")

recent = (
    df_enrollments
    .filter((F.col("course_id").isNotNull()) & (F.col("score") <= 77.0))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat(F.col("term"), F.lit("#"), F.col("course_id").cast("string")).alias("term_ref"),
        F.round(F.col("score") * 0.25, 2).alias("score_adj"),
        F.upper(F.trim(F.col("term"))).alias("term_clean"),
    )
)

line_level = (
    recent.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.enrolled_at",
        "s.term_ref",
        "s.term_clean",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.course_id").alias("enrollment_course_id"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.term").alias("enrollment_term"),
    )
)

without_matches = (
    line_level.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.score") >= 36.6) & (F.col("e.student_id") >= 7), "left_anti")
)

stats = (
    without_matches
    .filter(F.lower(F.col("term_clean")).like("%20%"))
    .groupBy("term")
    .agg(
        F.min("enrolled_at").alias("first_enrolled_at"),
        F.count(F.when(F.col("enrollment_term").isNotNull(), 1)).alias("n_enrollment_term_is_not_null"),
        F.percentile_approx("enrollment_score", 0.5).alias("median_enrollment_score"),
    )
)

metrics = (
    stats
    .select(
        "term",
        "first_enrolled_at",
        "n_enrollment_term_is_not_null",
        F.row_number().over(w_n_enrollment_term_is_not_nul).alias("seq_no"),
        F.ntile(10).over(w_term_by_median_enrollment_sc).alias("median_enrollment_score_decile"),
    )
)

annotated = metrics.withColumn("first_enrolled_at_date", F.to_date(F.col("first_enrolled_at")))

qualified = (
    annotated
    .crossJoin(annotated.agg(F.max("median_enrollment_score_decile").alias("overall_max_median_enrollment_score_decile")))
    .filter(F.col("median_enrollment_score_decile") < F.col("overall_max_median_enrollment_score_decile"))
    .drop("overall_max_median_enrollment_score_decile")
)

result = (
    qualified
    .select(
        "term",
        "seq_no",
        "median_enrollment_score_decile",
        "first_enrolled_at_date",
        F.length(F.trim(F.col("term"))).alias("term_len"),
    )
    .orderBy(F.col("first_enrolled_at_date").desc())
)
