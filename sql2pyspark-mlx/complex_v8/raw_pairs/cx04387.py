from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_enrolled_at = Window.orderBy(F.col("enrolled_at").desc(), "id")
w_course_id_enrolled_at_month = Window.partitionBy("course_id", F.month("enrolled_at")).orderBy("enrolled_at")
w_course_id_enrolled_at_month_2 = Window.partitionBy("course_id", F.month("enrolled_at")).orderBy("enrolled_at", "id")
w_all_by_seq_no_running = Window.orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_student_id_term_by_enrolled = Window.partitionBy("student_id", "term").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_seq_no_ties = Window.orderBy("seq_no")
w_all_by_seq_no = Window.orderBy("seq_no", "id")
w_grade_status_seq_no_2 = Window.partitionBy("grade_status", "seq_no_2")
w_student_id_term_by_enrolled_2 = Window.partitionBy("student_id", "term").orderBy(F.col("enrolled_at").desc())
w_all_by_total_minutes_since_p = Window.orderBy(F.col("total_minutes_since_prev_enrolled_at").desc())
w_all_by_total_minutes_since_p_2 = Window.orderBy("total_minutes_since_prev_enrolled_at", "last_known_seq_no", "student_id")

cleaned_enrollments = (
    df_enrollments
    .filter(F.col("term").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.date_format(F.col("enrolled_at"), "yyyy-MM-dd HH:00").alias("enrolled_at_label"),
    )
)

sequenced = (
    cleaned_enrollments
    .select(
        "*",
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_all_by_enrolled_at))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
        F.rank().over(w_course_id_enrolled_at_month).alias("enrolled_at_rank"),
        F.row_number().over(w_all_by_enrolled_at).alias("seq_no"),
        F.row_number().over(w_course_id_enrolled_at_month_2).alias("seq_no_2"),
    )
)

metrics = (
    sequenced
    .select(
        "*",
        F.last("seq_no", ignorenulls=True).over(w_all_by_seq_no_running).alias("last_known_seq_no"),
        F.count(F.lit(1)).over(w_student_id_term_by_enrolled).alias("running_count"),
        F.round(F.cume_dist().over(w_all_by_seq_no_ties), 3).alias("seq_no_cume_dist"),
        F.lead("seq_no_2", 3).over(w_all_by_seq_no).alias("next3_seq_no_2"),
        F.sum("seq_no_2").over(w_grade_status_seq_no_2).alias("grade_status_seq_no_2_sum_seq_no_2"),
        F.round(F.cume_dist().over(w_student_id_term_by_enrolled_2), 3).alias("enrolled_at_cume_dist"),
    )
)

first_rows = metrics.filter(F.col("seq_no_2") <= 5)

totals = (
    first_rows
    .groupBy("last_known_seq_no", "student_id")
    .agg(
        F.round(F.sum("next3_seq_no_2") / F.nullif(F.sum("grade_status_seq_no_2_sum_seq_no_2"), F.lit(0)), 3).alias("next3_seq_no_2_to_grade_status_seq_no_2_sum_seq_"),
        F.sum(F.when(~F.col("grade_status").isin("PASS", "WITHDRAWN"), 1).otherwise(0)).alias("n_grade_status_not_in_pass_wit"),
        F.round(F.sum("minutes_since_prev_enrolled_at"), 2).alias("total_minutes_since_prev_enrolled_at"),
        F.count(F.when(F.col("term") != "2025T1", 1)).alias("n_term_2025t1"),
    )
)

with_windows = (
    totals
    .select(
        "last_known_seq_no",
        "student_id",
        "n_grade_status_not_in_pass_wit",
        "n_term_2025t1",
        F.round(F.percent_rank().over(w_all_by_total_minutes_since_p), 3).alias("total_minutes_since_prev_enrolled_at_percent_ran"),
        F.lag("n_term_2025t1").over(w_all_by_total_minutes_since_p_2).alias("prev_n_term_2025t1"),
    )
)

screened = (
    with_windows
    .crossJoin(with_windows.agg(F.max("prev_n_term_2025t1").alias("overall_max_prev_n_term_2025t1")))
    .filter(F.col("prev_n_term_2025t1") < F.col("overall_max_prev_n_term_2025t1"))
    .drop("overall_max_prev_n_term_2025t1")
)

result = (
    screened
    .select(
        "last_known_seq_no",
        "student_id",
        "n_term_2025t1",
        F.round(F.col("n_term_2025t1") / F.nullif(F.col("prev_n_term_2025t1"), F.lit(0)), 3).alias("n_term_2025t1_per_prev_n_term_2025t1"),
    )
    .orderBy(F.col("student_id").desc(), F.col("last_known_seq_no").desc())
)
