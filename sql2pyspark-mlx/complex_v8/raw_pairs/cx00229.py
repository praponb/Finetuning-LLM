from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term_or_unknown = Window.partitionBy("term_or_unknown").orderBy("enrolled_at", "id")
w_term_or_unknown_running = Window.partitionBy("term_or_unknown").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_student_id = Window.partitionBy("student_id")
w_all_by_prev_enrolled_at_full = Window.orderBy(F.col("prev_enrolled_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_prev_enrolled_at_prio = Window.orderBy(F.col("prev_enrolled_at").asc_nulls_last(), "id").rowsBetween(-6, -1)

valid_enrollments = (
    df_enrollments
    .filter(F.col("course_id") <= 5)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("term"), F.lit("UNKNOWN")).alias("term_or_unknown"),
    )
)

with_prev = valid_enrollments.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_term_or_unknown))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_enrolled_at").isNull() | ((F.unix_timestamp("enrolled_at") - F.unix_timestamp("prev_enrolled_at")) / 60 > 90), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_term_or_unknown_running))

with_windows = (
    sessionized
    .select(
        "*",
        F.count(F.lit(1)).over(w_student_id).alias("rows_in_student_id"),
        F.nth_value("enrolled_at", 2).over(w_all_by_prev_enrolled_at_full).alias("second_enrolled_at"),
        F.round(F.col("burst_id") - F.avg("burst_id").over(w_student_id), 2).alias("burst_id_vs_student_id_avg"),
        F.round(F.avg("burst_id").over(w_all_by_prev_enrolled_at_prio), 2).alias("burst_id_avg_6_prior"),
    )
)

result = (
    with_windows
    .rollup("course_id", "term_or_unknown")
    .agg(
        F.sum("rows_in_student_id").alias("total_rows_in_student_id"),
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("grade_status") == "FAIL", F.col("score")).otherwise(0)).alias("score_grade_status_fail"),
    )
    .select(
        "course_id",
        F.coalesce(F.col("term_or_unknown"), F.lit("ALL")).alias("term_or_unknown"),
        "total_rows_in_student_id",
        "row_count",
        "score_grade_status_fail",
    )
)
