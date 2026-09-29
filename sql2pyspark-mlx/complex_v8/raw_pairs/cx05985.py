from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status = Window.partitionBy("grade_status").orderBy("enrolled_at", "id")
w_grade_status_running = Window.partitionBy("grade_status").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned_enrollments = (
    df_enrollments
    .filter(F.col("enrolled_at") >= F.lit("2025-02-06 10:00:00").cast("timestamp"))
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

with_prev = cleaned_enrollments.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_grade_status))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_enrolled_at").isNull() | (F.col("enrolled_at") > F.col("prev_enrolled_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("starts_session").over(w_grade_status_running))

result = (
    sessionized
    .filter(F.col("starts_session") > 0)
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "prev_enrolled_at",
        "starts_session",
        "session_id",
    )
    .orderBy(F.col("session_id").asc_nulls_last(), F.col("grade_status").desc())
)
