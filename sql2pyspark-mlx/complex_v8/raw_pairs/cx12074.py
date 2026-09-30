from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status = Window.partitionBy("grade_status").orderBy("enrolled_at", "id")
w_grade_status_running = Window.partitionBy("grade_status").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_enrollments = (
    df_enrollments
    .filter(~F.col("term").isin("2025T2", "2024T2"))
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

with_prev = filtered_enrollments.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_grade_status))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_enrolled_at").isNull() | (F.col("enrolled_at") > F.col("prev_enrolled_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_grade_status_running))

per_session = (
    sessionized
    .groupBy("grade_status", "session_id")
    .agg(
        F.min("enrolled_at").alias("session_start"),
        F.max("enrolled_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.round((F.unix_timestamp(F.max("enrolled_at")) - F.unix_timestamp(F.min("enrolled_at"))) / 60, 1).alias("duration_min"),
    )
)

qualified = (
    per_session
    .crossJoin(per_session.agg(F.avg("n_events").alias("overall_avg_n_events")))
    .filter(F.col("n_events") > F.col("overall_avg_n_events"))
    .drop("overall_avg_n_events")
)

tagged = (
    qualified
    .filter(F.col("grade_status") == "INCOMPLETE")
    .select(
        "grade_status",
        "session_id",
        "session_end",
        "n_events",
        "duration_min",
        F.greatest(F.col("session_id"), F.col("duration_min")).alias("greatest_session_id_duration_min"),
        F.least(F.col("duration_min"), F.col("n_events")).alias("least_duration_min_n_events"),
        F.coalesce(F.col("duration_min"), F.lit(0.0)).alias("duration_min_filled"),
    )
)

result = (
    tagged
    .select(
        "grade_status",
        "session_id",
        "session_end",
        "n_events",
        "duration_min",
        "least_duration_min_n_events",
        "duration_min_filled",
        F.when(F.col("n_events") >= 5, "A").when(F.col("n_events") >= 3, "B").otherwise("C").alias("n_events_tier"),
        F.round(F.col("duration_min") / F.nullif(F.col("duration_min_filled"), F.lit(0)), 3).alias("duration_min_per_duration_min_filled"),
    )
    .orderBy(F.col("n_events_tier").desc(), "duration_min_filled")
)
