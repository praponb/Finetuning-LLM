from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_score_tier = Window.partitionBy("score_tier").orderBy("enrolled_at", "id")
w_score_tier_running = Window.partitionBy("score_tier").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.when(F.col("score") >= 73.7, "A").when(F.col("score") >= 38.3, "B").otherwise("C").alias("score_tier"),
    )
)

with_prev = filtered.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_score_tier))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_enrolled_at").isNull() | (F.col("enrolled_at") > F.col("prev_enrolled_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("new_session_flag").over(w_score_tier_running))

per_session = (
    sessionized
    .groupBy("score_tier", "burst_id")
    .agg(
        F.min("enrolled_at").alias("session_start"),
        F.max("enrolled_at").alias("session_end"),
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("enrolled_at")) - F.unix_timestamp(F.min("enrolled_at"))) / 60, 1).alias("duration_min"),
    )
)

with_activity = (
    per_session
    .crossJoin(per_session.agg(F.max("burst_id").alias("overall_max_burst_id")))
    .filter(F.col("burst_id") < F.col("overall_max_burst_id"))
    .drop("overall_max_burst_id")
)

result = (
    with_activity
    .select(
        "score_tier",
        "burst_id",
        "session_end",
        "events",
        "duration_min",
    )
    .orderBy(F.col("duration_min").desc(), F.col("events").desc())
)
