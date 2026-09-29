from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_n_rows = Window.partitionBy("n_rows")
w_distinct_grade_status_studen = Window.partitionBy("distinct_grade_status", "student_id").orderBy(F.col("avg_enrollment_score").desc_nulls_first(), "course_dept", "enrolled_at_month", "student_id")

filtered = (
    df_enrollments
    .filter(F.col("course_id") >= 10)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.abs(F.col("score") - 32.9).alias("score_distance"),
    )
)

with_details_course = (
    filtered.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select("s.*", F.col("c.dept").alias("course_dept"))
)

derived = (
    with_details_course
    .filter(F.col("grade_status").isin("FAIL", "INCOMPLETE", "PASS"))
    .select(
        "*",
        (F.col("score") - F.col("score_distance")).alias("score_minus_score_distance"),
        F.length(F.trim(F.col("term"))).alias("term_len"),
    )
)

grouped = (
    derived
    .groupBy(
        "course_dept",
        F.date_trunc("month", "enrolled_at").alias("enrolled_at_month"),
        "student_id",
    )
    .agg(
        F.countDistinct("grade_status").alias("distinct_grade_status"),
        F.countDistinct("term").alias("distinct_terms"),
        F.min("score_distance").alias("min_score_distance"),
        F.count("*").alias("n_rows"),
        F.sum("score_minus_score_distance").alias("total_score_minus_score_distance"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.count("*").alias("enrollment_count"),
        F.round(F.avg("score"), 2).alias("avg_enrollment_score"),
    )
)

summarized_rows = (
    grouped.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
        "e.avg_enrollment_score",
    )
)

with_trends = (
    summarized_rows
    .select(
        "course_dept",
        "enrolled_at_month",
        "student_id",
        "n_rows",
        "enrollment_count",
        "avg_enrollment_score",
        F.count(F.lit(1)).over(w_n_rows).alias("rows_in_n_rows"),
        F.size(F.collect_set("course_dept").over(w_n_rows)).alias("distinct_course_dept_in_n_rows"),
        (F.col("n_rows") - F.lag("n_rows").over(w_distinct_grade_status_studen)).alias("n_rows_change"),
    )
)

result = (
    with_trends
    .filter(F.col("n_rows") > 2)
    .select(
        "course_dept",
        "enrolled_at_month",
        "student_id",
        "n_rows",
        "avg_enrollment_score",
        "rows_in_n_rows",
        "distinct_course_dept_in_n_rows",
        "n_rows_change",
        F.date_format(F.col("enrolled_at_month"), "MMM yyyy").alias("enrolled_at_month_label"),
        F.round(F.col("avg_enrollment_score") * 1.07, 2).alias("avg_enrollment_score_adj"),
    )
    .orderBy(F.col("enrolled_at_month").asc_nulls_last(), "course_dept", "student_id")
    .limit(5)
)
