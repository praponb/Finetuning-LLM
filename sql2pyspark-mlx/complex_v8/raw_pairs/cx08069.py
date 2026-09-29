from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status = Window.partitionBy("grade_status").orderBy("enrolled_at", "id")
w_grade_status_running = Window.partitionBy("grade_status").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat(F.col("term"), F.lit("#"), F.col("id").cast("string")).alias("term_ref"),
    )
)

with_prev = cleaned.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_grade_status))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_enrolled_at").isNull() | (F.col("enrolled_at") > F.col("prev_enrolled_at") + F.expr("INTERVAL 60 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_grade_status_running))

sessions = (
    sessionized
    .groupBy("grade_status", "session_no")
    .agg(
        F.min("enrolled_at").alias("session_start"),
        F.max("enrolled_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sum("score").alias("session_score"),
        F.round((F.unix_timestamp(F.max("enrolled_at")) - F.unix_timestamp(F.min("enrolled_at"))) / 60, 1).alias("duration_min"),
    )
)

filtered = (
    sessions
    .crossJoin(sessions.agg(F.min("session_score").alias("overall_min_session_score")))
    .filter(F.col("session_score") > F.col("overall_min_session_score"))
    .drop("overall_min_session_score")
)

result = (
    filtered
    .select(
        "grade_status",
        "session_no",
        "session_start",
        "session_end",
        "event_count",
    )
    .orderBy(
        F.col("session_start").asc(),
        F.col("event_count").desc_nulls_first(),
        "grade_status",
        "session_no",
    )
    .limit(5)
)
