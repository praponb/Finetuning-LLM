from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id = Window.partitionBy("student_id").orderBy("enrolled_at", "id")
w_student_id_running = Window.partitionBy("student_id").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid_enrollments = (
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

with_prev = valid_enrollments.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_student_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_enrolled_at").isNull() | (F.col("enrolled_at") > F.col("prev_enrolled_at") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_student_id_running))

enriched = (
    sessionized
    .select(
        "*",
        F.when(F.col("session_no") >= 4, "HIGH").when(F.col("session_no") >= 2, "MEDIUM").otherwise("LOW").alias("session_no_tier"),
        (F.col("enrolled_at") + F.expr("INTERVAL 2 HOURS")).alias("enrolled_at_plus_2h"),
    )
)

result = (
    enriched
    .filter(F.col("enrolled_at") < F.lit("2025-03-08 08:00:00").cast("timestamp"))
    .select(
        "id",
        "course_id",
        "new_session_flag",
        "session_no",
        "session_no_tier",
        "enrolled_at_plus_2h",
    )
    .orderBy(F.col("id").desc(), F.col("session_no").asc())
)
