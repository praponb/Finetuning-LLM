from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status = Window.partitionBy("grade_status").orderBy("enrolled_at", "id")
w_grade_status_running = Window.partitionBy("grade_status").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_enrollments = (
    df_enrollments
    .filter(F.month(F.col("enrolled_at")).isin(1, 2))
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
    .withColumn("starts_session", F.when(F.col("prev_enrolled_at").isNull() | (F.col("enrolled_at") > F.col("prev_enrolled_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_grade_status_running))

per_session = (
    sessionized
    .groupBy("grade_status", "burst_id")
    .agg(
        F.min("enrolled_at").alias("session_start"),
        F.max("enrolled_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.sum("score").alias("session_score"),
        F.round((F.unix_timestamp(F.max("enrolled_at")) - F.unix_timestamp(F.min("enrolled_at"))) / 60, 1).alias("duration_min"),
    )
)

result = (
    per_session
    .filter(F.col("n_events") <= 4)
    .select(
        "grade_status",
        "burst_id",
        "session_start",
        "n_events",
        "session_score",
        "duration_min",
        F.round(F.col("burst_id") / F.nullif(F.col("duration_min"), F.lit(0)), 3).alias("burst_id_per_duration_min"),
        F.round((F.unix_timestamp(F.col("session_end")) - F.unix_timestamp(F.col("session_start"))) / 3600, 1).alias("hours_session_start_to_session_end"),
    )
)
