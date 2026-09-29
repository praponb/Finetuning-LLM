from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id = Window.partitionBy("student_id").orderBy("enrolled_at", "id")
w_student_id_running = Window.partitionBy("student_id").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent = (
    df_enrollments
    .filter(F.col("score") > 5.9)
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

with_prev = recent.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_student_id))

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
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("enrolled_at")) - F.unix_timestamp(F.min("enrolled_at"))) / 60, 1).alias("duration_min"),
    )
)

result = (
    per_session
    .filter(F.col("duration_min") >= 36.0)
    .select(
        "student_id",
        "session_start",
        "session_end",
        "events",
        "duration_min",
        (F.floor(F.col("duration_min") / 50) * 50).alias("duration_min_bucket"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("session_start")).alias("days_since_session_start"),
    )
    .orderBy(F.col("session_start").desc())
)
