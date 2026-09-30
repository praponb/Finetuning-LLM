from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term_course_id_by_enrolled_a = Window.partitionBy("term", "course_id").orderBy(F.col("enrolled_at").desc(), "id")

scoped = (
    df_enrollments
    .filter((F.col("term").isNotNull()) & (F.col("enrolled_at") >= F.lit("2025-01-31 08:00:00").cast("timestamp")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.date_format(F.col("enrolled_at"), "yyyy-MM-dd HH:00").alias("enrolled_at_label"),
    )
)

with_windows = (
    scoped
    .select(
        "*",
        F.dense_rank().over(w_term_course_id_by_enrolled_a).alias("enrolled_at_dense_rank"),
        F.round(F.cume_dist().over(w_term_course_id_by_enrolled_a), 3).alias("enrolled_at_cume_dist"),
        F.round(F.percent_rank().over(w_term_course_id_by_enrolled_a), 3).alias("enrolled_at_percent_rank"),
    )
)

enriched_course = (
    with_windows.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        "s.enrolled_at_label",
        "s.enrolled_at_dense_rank",
        "s.enrolled_at_cume_dist",
        "s.enrolled_at_percent_rank",
        "c.course_code",
    )
)

student_id_stats = (
    enriched_course
    .groupBy("student_id")
    .agg(F.max("enrolled_at_dense_rank").alias("max_enrolled_at_dense_rank"))
)

vs_group = (
    enriched_course.alias("s")
    .join(student_id_stats.alias("g"), F.col("g.student_id") == F.col("s.student_id"), "inner")
    .filter(F.col("s.enrolled_at_dense_rank") == F.col("g.max_enrolled_at_dense_rank"))
    .select(
        "s.*",
        "g.max_enrolled_at_dense_rank",
        F.round(F.col("s.enrolled_at_dense_rank") - F.col("g.max_enrolled_at_dense_rank"), 2).alias("enrolled_at_dense_rank_minus_max"),
    )
)

result = (
    vs_group
    .groupBy("enrolled_at")
    .pivot("grade_status", ["WITHDRAWN", "PASS", "FAIL", "INCOMPLETE"])
    .agg(F.sum("enrolled_at_dense_rank_minus_max"))
    .orderBy("enrolled_at")
)
