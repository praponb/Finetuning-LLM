from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id = Window.partitionBy("student_id").orderBy("enrolled_at", "id")
w_student_id_running = Window.partitionBy("student_id").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.when(F.col("score") >= 77.0, "large").when(F.col("score") >= 26.2, "medium").otherwise("small").alias("score_tier"),
    )
)

with_prev = scoped.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_student_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_enrolled_at").isNull() | (F.col("enrolled_at") > F.col("prev_enrolled_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("new_session_flag").over(w_student_id_running))

sessions = (
    sessionized
    .groupBy("student_id", "burst_id")
    .agg(
        F.min("enrolled_at").alias("session_start"),
        F.max("enrolled_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.sum("score").alias("session_score"),
        F.round((F.unix_timestamp(F.max("enrolled_at")) - F.unix_timestamp(F.min("enrolled_at"))) / 60, 1).alias("duration_min"),
    )
)

stats = (
    sessions
    .groupBy("burst_id", "student_id")
    .agg(
        F.countDistinct("student_id").alias("distinct_student_ids"),
        F.percentile_approx("n_events", 0.5).alias("median_n_events"),
        F.sum("burst_id").alias("total_burst_id"),
        F.min("session_end").alias("first_session_end"),
        F.sum("duration_min").alias("sum_duration_min_check"),
    )
    .filter(F.col("sum_duration_min_check") > 72.0)
    .drop("sum_duration_min_check")
)

result = (
    stats
    .select(
        "burst_id",
        "student_id",
        "distinct_student_ids",
        "median_n_events",
        "total_burst_id",
        "first_session_end",
    )
    .orderBy(F.col("total_burst_id").desc(), F.col("burst_id").asc_nulls_last())
)
