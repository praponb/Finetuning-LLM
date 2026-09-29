from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term = Window.partitionBy("term").orderBy("enrolled_at", "id")
w_term_running = Window.partitionBy("term").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_enrollments = (
    df_enrollments
    .filter(F.hour(F.col("enrolled_at")).between(9, 13))
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

with_prev = recent_enrollments.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_term))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_enrolled_at").isNull() | (F.col("enrolled_at") > F.col("prev_enrolled_at") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("starts_session").over(w_term_running))

sessions = (
    sessionized
    .groupBy("term", "session_id")
    .agg(
        F.min("enrolled_at").alias("session_start"),
        F.max("enrolled_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.round((F.unix_timestamp(F.max("enrolled_at")) - F.unix_timestamp(F.min("enrolled_at"))) / 60, 1).alias("duration_min"),
    )
)

qualified = (
    sessions
    .crossJoin(sessions.agg(F.max("n_events").alias("overall_max_n_events")))
    .filter(F.col("n_events") < F.col("overall_max_n_events"))
    .drop("overall_max_n_events")
)

scored = (
    qualified
    .select(
        "term",
        "session_id",
        "session_start",
        "session_end",
        "duration_min",
        F.round(F.col("n_events") / F.nullif(F.col("session_id"), F.lit(0)), 3).alias("n_events_per_session_id"),
        F.when(F.col("session_id") >= 4, "HIGH").when(F.col("session_id") >= 2, "MEDIUM").otherwise("LOW").alias("session_id_tier"),
        F.abs(F.col("n_events") - 4).alias("n_events_distance"),
    )
)

result = (
    scored
    .cube("term", "session_id_tier")
    .agg(
        F.round(F.avg("n_events_distance"), 2).alias("avg_n_events_distance"),
        F.array_sort(F.collect_set("term")).alias("term_list"),
    )
    .select(
        F.coalesce(F.col("term"), F.lit("ALL")).alias("term"),
        F.coalesce(F.col("session_id_tier"), F.lit("ALL")).alias("session_id_tier"),
        "avg_n_events_distance",
        "term_list",
    )
    .orderBy(
        F.col("term").asc_nulls_last(),
        F.col("session_id_tier").asc_nulls_last(),
    )
)
