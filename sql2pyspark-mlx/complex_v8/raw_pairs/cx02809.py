from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status = Window.partitionBy("grade_status").orderBy("enrolled_at", "id")
w_grade_status_running = Window.partitionBy("grade_status").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_burst_id_grade_status = Window.partitionBy("burst_id", "grade_status")
w_burst_id_session_end_month_b = Window.partitionBy("burst_id", F.month("session_end")).orderBy("event_count", "grade_status", "burst_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered_enrollments = (
    df_enrollments
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

with_prev = filtered_enrollments.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_grade_status))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_enrolled_at").isNull() | (F.col("enrolled_at") > F.col("prev_enrolled_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_grade_status_running))

per_session = (
    sessionized
    .groupBy("grade_status", "burst_id")
    .agg(
        F.min("enrolled_at").alias("session_start"),
        F.max("enrolled_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sum("score").alias("session_score"),
        F.round((F.unix_timestamp(F.max("enrolled_at")) - F.unix_timestamp(F.min("enrolled_at"))) / 60, 1).alias("duration_min"),
    )
)

sequenced = (
    per_session
    .select(
        "*",
        F.round(F.avg("event_count").over(w_burst_id_grade_status), 2).alias("burst_id_grade_status_avg_event_count"),
        F.nth_value("event_count", 2).over(w_burst_id_session_end_month_b).alias("second_event_count"),
    )
)

qualified = (
    sequenced
    .crossJoin(sequenced.agg(F.avg("session_score").alias("overall_avg_session_score")))
    .filter(F.col("session_score") > F.col("overall_avg_session_score"))
    .drop("overall_avg_session_score")
)

result = (
    qualified
    .select(
        "grade_status",
        "burst_id",
        "session_start",
        "session_end",
        "event_count",
        "session_score",
        "duration_min",
        "burst_id_grade_status_avg_event_count",
        "second_event_count",
    )
)
