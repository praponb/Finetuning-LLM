from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status = Window.partitionBy("grade_status").orderBy("enrolled_at", "id")
w_grade_status_running = Window.partitionBy("grade_status").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_session_end_month_by_session = Window.partitionBy(F.month("session_end")).orderBy("session_start", "grade_status", "session_no").rowsBetween(-1, -1)
w_session_start_month_session = Window.partitionBy(F.date_trunc("month", "session_start"), "session_no")
w_session_end_month_by_session_2 = Window.partitionBy(F.month("session_end")).orderBy("session_start")

cleaned = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.concat_ws(" / ", F.col("term"), F.col("grade_status")).alias("term_grade_status_label"),
    )
)

with_prev = cleaned.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_grade_status))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_enrolled_at").isNull() | (F.col("enrolled_at") > F.col("prev_enrolled_at") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_grade_status_running))

session_summary = (
    sessionized
    .groupBy("grade_status", "session_no")
    .agg(
        F.min("enrolled_at").alias("session_start"),
        F.max("enrolled_at").alias("session_end"),
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("enrolled_at")) - F.unix_timestamp(F.min("enrolled_at"))) / 60, 1).alias("duration_min"),
    )
)

with_trends = (
    session_summary
    .select(
        "*",
        F.max("events").over(w_session_end_month_by_session).alias("events_max_1_prior"),
        F.count(F.lit(1)).over(w_session_start_month_session).alias("rows_in_session_start_month_session_no"),
        F.round(F.percent_rank().over(w_session_end_month_by_session_2), 3).alias("session_start_percent_rank"),
        F.min("duration_min").over(w_session_start_month_session).alias("session_start_month_session_no_min_duration_min"),
        F.size(F.collect_set("grade_status").over(w_session_start_month_session)).alias("distinct_grade_status_in_session_start_month_ses"),
    )
)

result = (
    with_trends
    .groupBy("session_no", "session_end")
    .pivot("grade_status", ["INCOMPLETE", "FAIL"])
    .agg(F.avg("distinct_grade_status_in_session_start_month_ses").alias("avg_v"), F.sum("distinct_grade_status_in_session_start_month_ses").alias("sum_v"))
)
