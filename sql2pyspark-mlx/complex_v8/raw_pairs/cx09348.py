from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term = Window.partitionBy("term").orderBy("enrolled_at", "id")
w_term_running = Window.partitionBy("term").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent = (
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

with_prev = recent.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_term))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_enrolled_at").isNull() | ((F.unix_timestamp("enrolled_at") - F.unix_timestamp("prev_enrolled_at")) / 60 > 90), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("starts_session").over(w_term_running))

result = (
    sessionized
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        "prev_enrolled_at",
        "starts_session",
        "session_id",
        F.substring(F.trim(F.col("grade_status")), 1, 3).alias("grade_status_prefix"),
    )
    .orderBy(F.col("session_id").desc())
)
