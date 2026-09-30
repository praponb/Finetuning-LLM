from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term = Window.partitionBy("term").orderBy("enrolled_at", "id")
w_term_running = Window.partitionBy("term").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_enrollments = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat_ws(" / ", F.col("grade_status"), F.col("term")).alias("grade_status_term_label"),
    )
)

with_prev = recent_enrollments.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_term))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_enrolled_at").isNull() | ((F.unix_timestamp("enrolled_at") - F.unix_timestamp("prev_enrolled_at")) / 60 > 60), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("starts_session").over(w_term_running))

per_session = (
    sessionized
    .groupBy("term", "session_id")
    .agg(
        F.min("enrolled_at").alias("session_start"),
        F.max("enrolled_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.sum("score").alias("session_score"),
        F.round((F.unix_timestamp(F.max("enrolled_at")) - F.unix_timestamp(F.min("enrolled_at"))) / 60, 1).alias("duration_min"),
    )
)

rollup_base = (
    per_session
    .filter(F.col("session_end") < F.lit("2025-02-05 09:00:00").cast("timestamp"))
    .groupBy(
        F.date_trunc("month", "session_start").alias("session_start_month"),
        "n_events",
        F.date_trunc("month", "session_end").alias("session_end_month"),
    )
    .agg(
        F.round(100 * F.count(F.when(F.col("term").isin("2024T1", "2025T2", "2024T2"), 1)) / F.count("*"), 1).alias("pct_term_in_2024t1_2025t2_2024t2"),
        F.max("session_start").alias("last_session_start"),
        F.round(F.sum("duration_min") / F.nullif(F.sum("session_score"), F.lit(0)), 3).alias("duration_min_to_session_score_ratio"),
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("term") != "2025T1", F.col("duration_min")).otherwise(0)).alias("duration_min_term_2025t1"),
        F.round(F.avg("duration_min"), 2).alias("avg_duration_min"),
    )
    .filter(F.col("row_count") >= 2)
)

result = (
    rollup_base
    .select(
        "session_start_month",
        "session_end_month",
        "pct_term_in_2024t1_2025t2_2024t2",
        "last_session_start",
        "duration_min_to_session_score_ratio",
        "avg_duration_min",
        F.greatest(F.col("n_events"), F.col("row_count")).alias("greatest_n_events_row_count"),
        F.dayofweek(F.col("session_end_month")).alias("session_end_month_dayofweek"),
    )
    .orderBy(
        F.col("session_start_month").desc(),
        F.col("session_end_month").desc_nulls_first(),
    )
)
