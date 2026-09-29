from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status = Window.partitionBy("grade_status").orderBy("enrolled_at", "id")
w_grade_status_running = Window.partitionBy("grade_status").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

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
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
    )
)

with_prev = recent.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_grade_status))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_enrolled_at").isNull() | ((F.unix_timestamp("enrolled_at") - F.unix_timestamp("prev_enrolled_at")) / 60 > 120), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("new_session_flag").over(w_grade_status_running))

per_session = (
    sessionized
    .groupBy("grade_status", "burst_id")
    .agg(
        F.min("enrolled_at").alias("session_start"),
        F.max("enrolled_at").alias("session_end"),
        F.count("*").alias("events"),
        F.sum("score").alias("session_score"),
        F.round((F.unix_timestamp(F.max("enrolled_at")) - F.unix_timestamp(F.min("enrolled_at"))) / 60, 1).alias("duration_min"),
    )
)

filtered = (
    per_session
    .crossJoin(per_session.agg(F.avg("session_score").alias("overall_avg_session_score")))
    .filter(F.col("session_score") > F.col("overall_avg_session_score"))
    .drop("overall_avg_session_score")
)

result = (
    filtered
    .select(
        "grade_status",
        "burst_id",
        "session_start",
        "session_end",
        "events",
        "session_score",
        "duration_min",
        (F.floor(F.col("session_score") / 50) * 50).alias("session_score_bucket"),
    )
    .orderBy("session_score", F.col("grade_status").asc_nulls_last(), "burst_id")
    .limit(20)
)
