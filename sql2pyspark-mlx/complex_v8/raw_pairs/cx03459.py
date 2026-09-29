from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_enrolled_at_month_term_by_sc = Window.partitionBy(F.date_trunc("month", "enrolled_at"), "term").orderBy(F.col("score").desc(), "id")

prepared = (
    df_enrollments
    .filter(F.col("score") >= 10.8)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat(F.col("term"), F.lit("#"), F.col("course_id").cast("string")).alias("term_ref"),
    )
)

grade_status_stats = (
    prepared
    .groupBy("grade_status")
    .agg(F.max("score").alias("max_score"))
)

above_average = (
    prepared.alias("s")
    .join(grade_status_stats.alias("g"), F.col("g.grade_status") == F.col("s.grade_status"), "inner")
    .filter(F.col("s.score") == F.col("g.max_score"))
    .select(
        "s.*",
        "g.max_score",
        F.round(F.col("s.score") - F.col("g.max_score"), 2).alias("score_minus_max"),
    )
)

qualified = (
    above_average.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left_semi")
    .filter(F.col("grade_status").isin("PASS", "INCOMPLETE"))
)

labeled = (
    qualified
    .filter(F.col("term_ref").isNotNull())
    .withColumn("score_minus_max_adj", F.round(F.col("score_minus_max") * 0.9, 2))
)

with_windows = (
    labeled
    .select(
        "id",
        "student_id",
        "term",
        "grade_status",
        "max_score",
        "score_minus_max",
        F.row_number().over(w_enrolled_at_month_term_by_sc).alias("score_seq"),
        F.rank().over(w_enrolled_at_month_term_by_sc).alias("score_rank"),
    )
)

result = (
    with_windows
    .filter(F.col("student_id").isNotNull())
    .select(
        "id",
        "student_id",
        "term",
        "score_seq",
        F.round(F.col("max_score") / F.nullif(F.col("score_rank"), F.lit(0)), 3).alias("max_score_per_score_rank"),
        F.round(F.col("max_score") / F.nullif(F.col("score_rank"), F.lit(0)), 3).alias("max_score_per_score_rank_2"),
    )
    .orderBy("max_score_per_score_rank_2")
)
