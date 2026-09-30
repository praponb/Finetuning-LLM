from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id = Window.partitionBy("student_id").orderBy("enrolled_at", "id")
w_student_id_running = Window.partitionBy("student_id").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned_enrollments = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

with_prev = cleaned_enrollments.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_student_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_enrolled_at").isNull() | (F.col("enrolled_at") > F.col("prev_enrolled_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("starts_session").over(w_student_id_running))

per_session = (
    sessionized
    .groupBy("student_id", "session_id")
    .agg(
        F.min("enrolled_at").alias("session_start"),
        F.max("enrolled_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.round((F.unix_timestamp(F.max("enrolled_at")) - F.unix_timestamp(F.min("enrolled_at"))) / 60, 1).alias("duration_min"),
    )
)

labeled = (
    per_session
    .filter(F.col("n_events").between(2, 5))
    .select(
        "*",
        (F.col("n_events") + F.col("duration_min")).alias("n_events_plus_duration_min"),
    )
)

annotated = (
    labeled
    .withColumn("days_since_session_start", F.datediff(F.lit("2025-12-31").cast("date"), F.col("session_start")))
)

result = (
    annotated
    .cube("student_id", "n_events")
    .agg(
        F.max("session_start").alias("last_session_start"),
        F.sum("session_id").alias("total_session_id"),
    )
    .orderBy(
        F.col("student_id").asc_nulls_last(),
        F.col("n_events").asc_nulls_last(),
    )
)
