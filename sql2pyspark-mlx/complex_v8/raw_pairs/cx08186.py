from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status = Window.partitionBy("grade_status").orderBy("enrolled_at", "id")
w_grade_status_running = Window.partitionBy("grade_status").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned_enrollments = (
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

with_prev = cleaned_enrollments.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_grade_status))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_enrolled_at").isNull() | ((F.unix_timestamp("enrolled_at") - F.unix_timestamp("prev_enrolled_at")) / 60 > 30), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_grade_status_running))

qualified = (
    sessionized
    .crossJoin(sessionized.agg(F.avg("starts_session").alias("overall_avg_starts_session")))
    .filter(F.col("starts_session") > F.col("overall_avg_starts_session"))
    .drop("overall_avg_starts_session")
)

result = (
    qualified
    .filter(F.col("prev_enrolled_at") >= F.lit("2025-01-31 11:00:00").cast("timestamp"))
    .select(
        "id",
        "term",
        "score",
        "enrolled_at",
        "prev_enrolled_at",
        "starts_session",
        "burst_id",
    )
)
