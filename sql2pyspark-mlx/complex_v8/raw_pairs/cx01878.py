from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id = Window.partitionBy("student_id").orderBy("enrolled_at", "id")
w_student_id_running = Window.partitionBy("student_id").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_enrollment_count_2_by_sessio = Window.partitionBy("enrollment_count_2").orderBy("session_no", "id")
w_enrollment_count_2_by_sessio_2 = Window.partitionBy("enrollment_count_2").orderBy("session_no", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_prev_enrolled_at_to_date = Window.partitionBy(F.to_date("prev_enrolled_at"))
w_enrollment_count_2_by_sessio_3 = Window.partitionBy("enrollment_count_2").orderBy("session_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_enrollments
    .filter(F.col("course_id") >= 10)
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.upper(F.trim(F.col("term"))).alias("term_clean"),
    )
)

with_prev = valid.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_student_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_enrolled_at").isNull() | ((F.unix_timestamp("enrolled_at") - F.unix_timestamp("prev_enrolled_at")) / 60 > 30), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_student_id_running))

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.count("*").alias("enrollment_count"),
        F.sum("score").alias("total_enrollment_score"),
        F.count("*").alias("enrollment_count_2"),
    )
)

profiled = (
    sessionized.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
        "e.total_enrollment_score",
        F.coalesce(F.col("e.enrollment_count_2"), F.lit(0)).alias("enrollment_count_2"),
    )
)

with_windows = (
    profiled
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        "term_clean",
        "prev_enrolled_at",
        "new_session_flag",
        "session_no",
        "enrollment_count",
        "enrollment_count_2",
        F.row_number().over(w_enrollment_count_2_by_sessio).alias("seq_no"),
        F.nth_value("session_no", 2).over(w_enrollment_count_2_by_sessio_2).alias("second_session_no"),
        F.size(F.collect_set("grade_status").over(w_prev_enrolled_at_to_date)).alias("distinct_grade_status_in_prev_enrolled_at_to_dat"),
        F.first("new_session_flag", ignorenulls=True).over(w_enrollment_count_2_by_sessio_3).alias("first_known_new_session_flag"),
        (F.col("score") - F.lag("score").over(w_enrollment_count_2_by_sessio)).alias("score_change"),
        F.count(F.lit(1)).over(w_prev_enrolled_at_to_date).alias("rows_in_prev_enrolled_at_to_date"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "student_id",
        "score",
        "grade_status",
        "enrolled_at",
        "term_clean",
        "prev_enrolled_at",
        "new_session_flag",
        "session_no",
        "enrollment_count",
        "enrollment_count_2",
        "distinct_grade_status_in_prev_enrolled_at_to_dat",
        "first_known_new_session_flag",
        "score_change",
    )
    .orderBy(F.col("grade_status").desc_nulls_first(), "id")
    .limit(10)
)
