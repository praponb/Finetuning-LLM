from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_enrolled_at_mon = Window.partitionBy("grade_status", F.date_trunc("month", "enrolled_at"))
w_student_id_course_id_by_scor = Window.partitionBy("student_id", "course_id").orderBy("score", "id")
w_student_id_by_score_ties = Window.partitionBy("student_id").orderBy(F.col("score").desc())
w_student_id_by_enrolled_at_fu = Window.partitionBy("student_id").orderBy(F.col("enrolled_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_student_id = Window.partitionBy("student_id")
w_student_id_by_score_moving_2 = Window.partitionBy("student_id").orderBy(F.col("score").desc(), "id").rowsBetween(-2, Window.currentRow)

recent_enrollments = (
    df_enrollments
    .filter(F.month(F.col("enrolled_at")).isin(1, 3))
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

with_windows = (
    recent_enrollments
    .select(
        "*",
        F.round(F.col("score") - F.avg("score").over(w_grade_status_enrolled_at_mon), 2).alias("score_vs_grade_status_enrolled_at_month_avg"),
        F.lead("enrolled_at").over(w_student_id_course_id_by_scor).alias("next_enrolled_at"),
        F.round(F.percent_rank().over(w_student_id_by_score_ties), 3).alias("score_percent_rank"),
        F.first("enrolled_at").over(w_student_id_by_enrolled_at_fu).alias("first_enrolled_at"),
        F.size(F.collect_set("course_id").over(w_grade_status_enrolled_at_mon)).alias("distinct_course_id_in_grade_status_enrolled_at_m"),
        F.round(F.col("score") - F.avg("score").over(w_student_id), 2).alias("score_vs_student_id_avg"),
        F.sum("score").over(w_student_id).alias("student_id_sum_score"),
        F.max("score").over(w_student_id_by_score_moving_2).alias("score_max_3"),
    )
)

labeled = (
    with_windows
    .select(
        "*",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("first_enrolled_at")).alias("days_since_first_enrolled_at"),
        F.coalesce(F.col("score_percent_rank"), F.lit(0.0)).alias("score_percent_rank_filled"),
    )
)

summary = (
    labeled
    .groupBy(
        F.date_trunc("month", "first_enrolled_at").alias("first_enrolled_at_month"),
        "distinct_course_id_in_grade_status_enrolled_at_m",
        F.date_trunc("month", "next_enrolled_at").alias("next_enrolled_at_month"),
    )
    .agg(
        F.round(F.avg("score_max_3"), 2).alias("avg_score_max_3"),
        F.sum(F.when(F.col("grade_status") == "INCOMPLETE", 1).otherwise(0)).alias("n_grade_status_incomplete"),
    )
)

result = (
    summary
    .select(
        "first_enrolled_at_month",
        "distinct_course_id_in_grade_status_enrolled_at_m",
        "next_enrolled_at_month",
        "avg_score_max_3",
        F.date_trunc("day", F.col("next_enrolled_at_month")).alias("next_enrolled_at_month_day"),
    )
    .orderBy("next_enrolled_at_month")
)
