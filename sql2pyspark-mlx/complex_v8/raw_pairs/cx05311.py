from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_first_enrolled_at_year = Window.partitionBy(F.year("first_enrolled_at"))
w_row_count_n_grade_status_is = Window.partitionBy("row_count", "n_grade_status_is_not_null").orderBy(F.col("total_score").desc(), "course_id", "enrolled_at_quarter_ym")

prepared = (
    df_enrollments
    .filter((F.col("grade_status").isNotNull()) & (F.col("student_id") <= 3))
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.date_trunc("quarter", F.col("enrolled_at")).alias("enrolled_at_quarter"),
    )
)

summary = (
    prepared
    .groupBy(
        "course_id",
        F.date_format("enrolled_at_quarter", "yyyy-MM").alias("enrolled_at_quarter_ym"),
    )
    .agg(
        F.min("enrolled_at").alias("first_enrolled_at"),
        F.min("enrolled_at_quarter").alias("first_enrolled_at_quarter"),
        F.count(F.when(F.col("grade_status").isNotNull(), 1)).alias("n_grade_status_is_not_null"),
        F.sum("score").alias("total_score"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("row_count") >= 2)
)

filtered = (
    summary.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left_semi")
    .filter(F.col("course_id") >= 11)
)

screened = (
    filtered.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.score") >= 34.2) & (F.col("e.grade_status") != "PASS"), "left_anti")
)

with_trends = (
    screened
    .select(
        "course_id",
        "enrolled_at_quarter_ym",
        "first_enrolled_at",
        "n_grade_status_is_not_null",
        F.max("total_score").over(w_first_enrolled_at_year).alias("first_enrolled_at_year_max_total_score"),
        F.dense_rank().over(w_row_count_n_grade_status_is).alias("total_score_dense_rank"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(F.max("enrolled_at").alias("last_enrollment_enrolled_at"))
)

summarized_rows = (
    with_trends.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select("s.*", "e.last_enrollment_enrolled_at")
)

result = (
    summarized_rows
    .select(
        "course_id",
        "n_grade_status_is_not_null",
        "first_enrolled_at_year_max_total_score",
        "total_score_dense_rank",
        "last_enrollment_enrolled_at",
    )
    .orderBy(F.col("first_enrolled_at_year_max_total_score").asc_nulls_last())
)
