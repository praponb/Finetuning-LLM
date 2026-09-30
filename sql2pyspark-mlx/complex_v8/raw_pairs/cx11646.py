from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id = Window.partitionBy("student_id")
w_all_by_enrolled_at_date = Window.orderBy(F.col("enrolled_at_date").desc(), "id")
w_all_by_enrolled_at_date_prio = Window.orderBy(F.col("enrolled_at_date").desc(), "id").rowsBetween(-6, -1)
w_term_by_enrolled_at_moving_2 = Window.partitionBy("term").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(-2, Window.currentRow)
w_all_by_enrolled_at_date_full = Window.orderBy(F.col("enrolled_at_date").desc(), "id", "enrollment_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_enrollment_enrolled_at_month = Window.partitionBy(F.month("enrollment_enrolled_at"), "enrollment_id").orderBy("days_since_prev_enrolled_at_date", "id", "enrollment_id")
w_enrollment_enrolled_at_month_2 = Window.partitionBy(F.month("enrollment_enrolled_at"), "enrollment_id").orderBy("days_since_prev_enrolled_at_date", "id", "enrollment_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_enrolled_at_date_ties = Window.orderBy(F.col("enrolled_at_date").desc())
w_student_id_enrollment_grade = Window.partitionBy("student_id", "enrollment_grade_status")

base = (
    df_enrollments
    .filter((F.to_date(F.col("enrolled_at")) < F.lit("2025-02-06").cast("date")) & (F.col("score") < 62.5))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.substring(F.trim(F.col("grade_status")), 1, 3).alias("grade_status_prefix"),
        F.substring(F.trim(F.col("term")), 1, 3).alias("term_prefix"),
        F.to_date(F.col("enrolled_at")).alias("enrolled_at_date"),
    )
)

with_trends = (
    base
    .select(
        "*",
        F.count(F.lit(1)).over(w_student_id).alias("rows_in_student_id"),
        F.datediff(F.col("enrolled_at_date"), F.lag("enrolled_at_date").over(w_all_by_enrolled_at_date)).alias("days_since_prev_enrolled_at_date"),
        F.sum("score").over(w_all_by_enrolled_at_date_prio).alias("score_sum_6_prior"),
        F.sum("score").over(w_term_by_enrolled_at_moving_2).alias("score_sum_3"),
    )
)

line_level = (
    with_trends.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.enrolled_at") >= F.lit("2025-02-06 10:00:00").cast("timestamp")), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.grade_status_prefix",
        "s.term_prefix",
        "s.enrolled_at_date",
        "s.rows_in_student_id",
        "s.days_since_prev_enrolled_at_date",
        "s.score_sum_6_prior",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
        F.col("e.student_id").alias("enrollment_student_id"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
    )
)

with_windows = (
    line_level
    .select(
        "*",
        F.first("enrolled_at_date").over(w_all_by_enrolled_at_date_full).alias("first_enrolled_at_date"),
        F.ntile(4).over(w_enrollment_enrolled_at_month).alias("days_since_prev_enrolled_at_date_quartile"),
        F.nth_value("rows_in_student_id", 2).over(w_enrollment_enrolled_at_month_2).alias("second_rows_in_student_id"),
        F.dense_rank().over(w_all_by_enrolled_at_date_ties).alias("enrolled_at_date_dense_rank"),
        F.size(F.collect_set("enrollment_student_id").over(w_student_id_enrollment_grade)).alias("distinct_enrollment_student_id_in_student_id_enr"),
        F.round(F.cume_dist().over(w_all_by_enrolled_at_date_ties), 3).alias("enrolled_at_date_cume_dist"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status_prefix",
        "term_prefix",
        "enrolled_at_date",
        "rows_in_student_id",
        "days_since_prev_enrolled_at_date",
        "score_sum_6_prior",
        "enrollment_id",
        "enrollment_enrolled_at",
        "enrollment_student_id",
        "enrollment_grade_status",
        "first_enrolled_at_date",
        "days_since_prev_enrolled_at_date_quartile",
        "second_rows_in_student_id",
        "distinct_enrollment_student_id_in_student_id_enr",
        "enrolled_at_date_cume_dist",
        F.concat_ws("-", F.col("term_prefix"), F.col("enrollment_grade_status")).alias("term_prefix_enrollment_grade_status_label"),
    )
    .orderBy(
        F.col("term_prefix").desc(),
        "course_id",
        "id",
        "enrollment_id",
    )
    .limit(5)
)
