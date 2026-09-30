from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status = Window.partitionBy("grade_status").orderBy("enrolled_at", "id")
w_grade_status_running = Window.partitionBy("grade_status").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_session_end_month = Window.partitionBy(F.date_trunc("month", "session_end"))
w_event_count_by_session_end = Window.partitionBy("event_count").orderBy(F.col("session_end").desc(), "grade_status", "burst_id")

recent_enrollments = (
    df_enrollments
    .filter(F.col("score") > 6.2)
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.round(F.col("score") * 1.1, 2).alias("score_adj"),
    )
)

with_prev = recent_enrollments.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_grade_status))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_enrolled_at").isNull() | (F.col("enrolled_at") > F.col("prev_enrolled_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_grade_status_running))

sessions = (
    sessionized
    .groupBy("grade_status", "burst_id")
    .agg(
        F.min("enrolled_at").alias("session_start"),
        F.max("enrolled_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.round((F.unix_timestamp(F.max("enrolled_at")) - F.unix_timestamp(F.min("enrolled_at"))) / 60, 1).alias("duration_min"),
    )
)

metrics = (
    sessions
    .withColumn("session_end_month_min_duration_min", F.min("duration_min").over(w_session_end_month))
    .withColumn("session_end_seq", F.row_number().over(w_event_count_by_session_end))
)

screened = (
    metrics
    .crossJoin(metrics.agg(F.avg("burst_id").alias("overall_avg_burst_id")))
    .filter(F.col("burst_id") > F.col("overall_avg_burst_id"))
    .drop("overall_avg_burst_id")
)

result = (
    screened
    .select(
        "grade_status",
        "session_end",
        "event_count",
        "duration_min",
        "session_end_month_min_duration_min",
        "session_end_seq",
        F.round(F.col("duration_min") * 1.07, 2).alias("duration_min_adj"),
    )
    .orderBy(
        F.col("event_count").asc_nulls_last(),
        F.col("session_end_month_min_duration_min").asc(),
    )
)
