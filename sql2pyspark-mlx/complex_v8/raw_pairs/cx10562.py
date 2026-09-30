from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term_len = Window.partitionBy("term_len")

recent = (
    df_enrollments
    .filter(F.month(F.col("enrolled_at")).isin(1, 2))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.date_trunc("month", F.col("enrolled_at")).alias("enrolled_at_month"),
        F.length(F.trim(F.col("term"))).alias("term_len"),
        F.length(F.trim(F.col("term"))).alias("term_len_2"),
    )
)

qualified = (
    recent.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.enrolled_at") < F.lit("2025-02-24 08:00:00").cast("timestamp")), "left_anti")
)

with_trends = (
    qualified
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        "term_len",
        F.count(F.lit(1)).over(w_term_len).alias("rows_in_term_len"),
        F.size(F.collect_set("term").over(w_term_len)).alias("distinct_term_in_term_len"),
        F.max("term_len").over(w_term_len).alias("term_len_max_term_len"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        "term_len",
        "rows_in_term_len",
        "distinct_term_in_term_len",
        F.substring(F.trim(F.col("grade_status")), 1, 3).alias("grade_status_prefix"),
    )
)
