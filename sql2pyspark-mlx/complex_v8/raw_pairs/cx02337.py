from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_or_unknown = Window.partitionBy("grade_status_or_unknown")
w_student_id_grade_status_or_u = Window.partitionBy("student_id", "grade_status_or_unknown").orderBy("score_per_grade_status_len", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_enrollments
    .filter(F.hour(F.col("enrolled_at")).between(6, 10))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
    )
)

labeled = (
    base
    .filter(F.col("course_id").isNotNull())
    .select(
        "*",
        F.round(F.col("score") / F.nullif(F.col("grade_status_len"), F.lit(0)), 3).alias("score_per_grade_status_len"),
        F.coalesce(F.col("grade_status_len"), F.lit(0)).alias("grade_status_len_filled"),
    )
)

screened = (
    labeled.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.score") > 38.5), "left_anti")
)

student_id_stats = (
    screened
    .groupBy("student_id")
    .agg(F.avg("score_per_grade_status_len").alias("avg_score_per_grade_status_len"))
)

outliers = (
    screened.alias("s")
    .join(student_id_stats.alias("g"), F.col("g.student_id") == F.col("s.student_id"), "inner")
    .filter(F.col("s.score_per_grade_status_len") > F.col("g.avg_score_per_grade_status_len"))
    .select(
        "s.*",
        "g.avg_score_per_grade_status_len",
        F.round(F.col("s.score_per_grade_status_len") - F.col("g.avg_score_per_grade_status_len"), 2).alias("score_per_grade_status_len_minus_avg"),
    )
)

with_windows = (
    outliers
    .withColumn("rows_in_grade_status_or_unknown", F.count(F.lit(1)).over(w_grade_status_or_unknown))
    .withColumn("running_grade_status_len", F.sum("grade_status_len").over(w_student_id_grade_status_or_u))
)

screened_2 = (
    with_windows.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.score") >= 17.4) & (F.col("e.term") != "2024T2"), "left_semi")
)

result = (
    screened_2
    .select(
        "id",
        "score",
        "enrolled_at",
        "grade_status_or_unknown",
        "grade_status_len",
        "score_per_grade_status_len",
        "avg_score_per_grade_status_len",
        "rows_in_grade_status_or_unknown",
        "running_grade_status_len",
    )
    .orderBy(F.col("score").asc_nulls_last(), "running_grade_status_len")
)
