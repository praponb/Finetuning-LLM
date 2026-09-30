from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id = Window.partitionBy("course_id").orderBy("enrolled_at", "id")
w_course_id_running = Window.partitionBy("course_id").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_enrollments = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
    )
)

with_prev = recent_enrollments.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_course_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_enrolled_at").isNull() | (F.col("enrolled_at") > F.col("prev_enrolled_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("new_session_flag").over(w_course_id_running))

session_summary = (
    sessionized
    .groupBy("course_id", "burst_id")
    .agg(
        F.min("enrolled_at").alias("session_start"),
        F.max("enrolled_at").alias("session_end"),
        F.count("*").alias("events"),
        F.sum("score").alias("session_score"),
        F.round((F.unix_timestamp(F.max("enrolled_at")) - F.unix_timestamp(F.min("enrolled_at"))) / 60, 1).alias("duration_min"),
    )
)

tagged = (
    session_summary
    .select(
        "course_id",
        "burst_id",
        "session_start",
        "session_end",
        "events",
        (F.col("session_start") + F.expr("INTERVAL 1 DAYS")).alias("session_start_plus_1d"),
    )
)

rollup_base = (
    tagged
    .groupBy(
        "events",
        "burst_id",
        F.date_trunc("month", "session_end").alias("session_end_month"),
    )
    .agg(
        F.min("events").alias("min_events"),
        F.sum("events").alias("total_events"),
        F.round(F.avg("events"), 2).alias("avg_events"),
        F.countDistinct("course_id").alias("distinct_course_ids"),
    )
)

result = (
    rollup_base
    .select(
        "events",
        "burst_id",
        "session_end_month",
        "min_events",
        "total_events",
        "avg_events",
        "distinct_course_ids",
        (F.col("burst_id") + F.col("total_events")).alias("burst_id_plus_total_events"),
        F.to_date(F.col("session_end_month")).alias("session_end_month_date"),
    )
    .orderBy("avg_events")
)
