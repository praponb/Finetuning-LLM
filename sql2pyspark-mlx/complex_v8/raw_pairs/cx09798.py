from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status = Window.partitionBy("grade_status").orderBy("enrolled_at", "id")
w_grade_status_running = Window.partitionBy("grade_status").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_dedup = Window.partitionBy("term", "student_id").orderBy("starts_session", "id")

scoped_enrollments = (
    df_enrollments
    .filter(F.col("enrolled_at") < F.lit("2025-02-24 13:00:00").cast("timestamp"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
    )
)

with_prev = scoped_enrollments.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_grade_status))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_enrolled_at").isNull() | (F.col("enrolled_at") > F.col("prev_enrolled_at") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("starts_session").over(w_grade_status_running))

latest_per_key = (
    sessionized
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

student_id_stats = (
    latest_per_key
    .groupBy("student_id")
    .agg(F.max("session_id").alias("max_session_id"))
)

above_average = (
    latest_per_key.alias("s")
    .join(student_id_stats.alias("g"), F.col("g.student_id") == F.col("s.student_id"), "inner")
    .filter(F.col("s.session_id") == F.col("g.max_session_id"))
    .select(
        "s.*",
        "g.max_session_id",
        F.round(F.col("s.session_id") - F.col("g.max_session_id"), 2).alias("session_id_minus_max"),
    )
)

result = (
    above_average
    .select(
        "id",
        "student_id",
        "course_id",
        "grade_status",
        "enrolled_at",
        "starts_session",
        "session_id",
        "max_session_id",
        F.abs(F.col("starts_session") - 0).alias("starts_session_distance"),
        F.hour(F.col("prev_enrolled_at")).alias("prev_enrolled_at_hour"),
    )
    .orderBy("max_session_id", "student_id")
)
