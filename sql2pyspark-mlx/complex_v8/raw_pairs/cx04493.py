from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_score_tier = Window.partitionBy("score_tier").orderBy("enrolled_at", "id")
w_score_tier_running = Window.partitionBy("score_tier").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_session_id = Window.partitionBy("session_id")
w_score_tier_session_start_yea = Window.partitionBy("score_tier", F.year("session_start")).orderBy("session_start", "score_tier", "session_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid_enrollments = (
    df_enrollments
    .filter(F.col("enrolled_at") < F.lit("2025-03-08 14:00:00").cast("timestamp"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.when(F.col("score") >= 78.8, "HIGH").when(F.col("score") >= 38.1, "MEDIUM").otherwise("LOW").alias("score_tier"),
    )
)

with_prev = valid_enrollments.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_score_tier))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_enrolled_at").isNull() | ((F.unix_timestamp("enrolled_at") - F.unix_timestamp("prev_enrolled_at")) / 60 > 90), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_score_tier_running))

per_session = (
    sessionized
    .groupBy("score_tier", "session_id")
    .agg(
        F.min("enrolled_at").alias("session_start"),
        F.max("enrolled_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.sum("score").alias("session_score"),
        F.round((F.unix_timestamp(F.max("enrolled_at")) - F.unix_timestamp(F.min("enrolled_at"))) / 60, 1).alias("duration_min"),
    )
)

windowed = (
    per_session
    .withColumn("n_events_vs_session_id_avg", F.round(F.col("n_events") - F.avg("n_events").over(w_session_id), 2))
    .withColumn("second_n_events", F.nth_value("n_events", 2).over(w_score_tier_session_start_yea))
)

grouped = (
    windowed
    .groupBy("session_id")
    .agg(
        F.countDistinct("score_tier").alias("distinct_score_tiers"),
        F.count("*").alias("cnt"),
        F.percentile_approx("duration_min", 0.5).alias("median_duration_min"),
        F.round(F.stddev("duration_min"), 2).alias("stddev_duration_min"),
        F.sum("duration_min").alias("total_duration_min"),
        F.max("session_end").alias("last_session_end"),
    )
)

result = (
    grouped
    .select(
        "session_id",
        F.col("median_duration_min").alias("m_median_duration_min"),
        F.col("stddev_duration_min").alias("m_stddev_duration_min"),
    )
    .unpivot(
        ids=["session_id"],
        values=["m_median_duration_min", "m_stddev_duration_min"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .orderBy("session_id", "metric_name")
)
