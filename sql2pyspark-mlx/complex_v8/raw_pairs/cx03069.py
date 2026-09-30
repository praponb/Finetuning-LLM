from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term = Window.partitionBy("term").orderBy("enrolled_at", "id")
w_term_running = Window.partitionBy("term").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned_enrollments = (
    df_enrollments
    .filter(F.col("course_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.to_date(F.col("enrolled_at")).alias("enrolled_at_date"),
    )
)

with_prev = cleaned_enrollments.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_term))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_enrolled_at").isNull() | (F.col("enrolled_at") > F.col("prev_enrolled_at") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_term_running))

session_summary = (
    sessionized
    .groupBy("term", "burst_id")
    .agg(
        F.min("enrolled_at").alias("session_start"),
        F.max("enrolled_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sum("score").alias("session_score"),
        F.round((F.unix_timestamp(F.max("enrolled_at")) - F.unix_timestamp(F.min("enrolled_at"))) / 60, 1).alias("duration_min"),
    )
)

without_matches = (
    session_summary
    .crossJoin(session_summary.agg(F.avg("session_score").alias("overall_avg_session_score")))
    .filter(F.col("session_score") > F.col("overall_avg_session_score"))
    .drop("overall_avg_session_score")
)

qualified = (
    without_matches
    .crossJoin(without_matches.agg(F.min("session_score").alias("overall_min_session_score")))
    .filter(F.col("session_score") > F.col("overall_min_session_score"))
    .drop("overall_min_session_score")
)

result = (
    qualified
    .filter(F.col("burst_id") <= 4)
    .select(
        "term",
        "burst_id",
        "session_start",
        "session_end",
        "event_count",
        "session_score",
        F.coalesce(F.col("term"), F.lit("UNKNOWN")).alias("term_or_unknown"),
    )
    .orderBy(F.col("session_end").desc())
)
