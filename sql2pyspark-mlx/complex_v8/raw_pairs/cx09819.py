from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id_term_by_score = Window.partitionBy("student_id", "term").orderBy(F.col("score").desc(), "id")
w_enrolled_at_month_by_enrolle = Window.partitionBy(F.month("enrolled_at")).orderBy("enrolled_at")
w_student_id_grade_status = Window.partitionBy("student_id", "grade_status")
w_all_by_enrolled_at_ties = Window.orderBy("enrolled_at")
w_student_id_term_by_score_run = Window.partitionBy("student_id", "term").orderBy(F.col("score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent = (
    df_enrollments
    .filter(F.month(F.col("enrolled_at")).isin(1, 3))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

without_matches = (
    recent.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.term") == "2024T2"), "left_anti")
)

windowed = (
    without_matches
    .select(
        "*",
        F.rank().over(w_student_id_term_by_score).alias("score_rank"),
        F.round(F.cume_dist().over(w_enrolled_at_month_by_enrolle), 3).alias("enrolled_at_cume_dist"),
        F.min("score").over(w_student_id_grade_status).alias("student_id_grade_status_min_score"),
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_student_id_term_by_score))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
        F.rank().over(w_all_by_enrolled_at_ties).alias("enrolled_at_rank"),
        F.min("score").over(w_student_id_term_by_score_run).alias("min_score_so_far"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(F.count("*").alias("enrollment_count"))
)

with_stats = (
    windowed.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

result = (
    with_stats
    .rollup("course_id", "term")
    .agg(
        F.grouping("course_id").alias("is_total_course_id"),
        F.count("*").alias("n_rows"),
        F.min("enrolled_at").alias("first_enrolled_at"),
        F.min("min_score_so_far").alias("min_min_score_so_far"),
    )
    .orderBy(F.col("course_id").asc_nulls_last(), F.col("term").asc_nulls_last())
)
