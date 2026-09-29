from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term_course_id_by_score_full = Window.partitionBy("term", "course_id").orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_enrolled_at_month_term = Window.partitionBy(F.date_trunc("month", "enrolled_at"), "term")

valid_enrollments = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
    )
)

metrics = (
    valid_enrollments
    .select(
        "*",
        F.first("score").over(w_term_course_id_by_score_full).alias("first_score"),
        F.count(F.lit(1)).over(w_enrolled_at_month_term).alias("rows_in_enrolled_at_month_term"),
    )
)

filtered = (
    metrics.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.grade_status").isin("WITHDRAWN", "PASS")), "left_anti")
)

with_activity = (
    filtered.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.student_id").isNotNull()) & (F.col("e.score").between(26.2, 73.0)), "left_anti")
)

result = (
    with_activity
    .filter(F.col("student_id") >= 4)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
    )
    .orderBy(F.col("student_id").asc_nulls_last(), F.col("course_id").desc(), "id")
    .limit(10)
)
