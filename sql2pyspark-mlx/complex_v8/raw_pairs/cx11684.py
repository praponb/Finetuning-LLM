from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status = Window.partitionBy("grade_status").orderBy("enrolled_at", "id")
w_grade_status_running = Window.partitionBy("grade_status").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_duration_min = Window.orderBy("duration_min", "grade_status", "session_id")
w_grade_status_session_end_mon = Window.partitionBy("grade_status", F.month("session_end"))

valid = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat(F.col("term"), F.lit("#"), F.col("course_id").cast("string")).alias("term_ref"),
    )
)

with_prev = valid.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_grade_status))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_enrolled_at").isNull() | (F.col("enrolled_at") > F.col("prev_enrolled_at") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_grade_status_running))

session_summary = (
    sessionized
    .groupBy("grade_status", "session_id")
    .agg(
        F.min("enrolled_at").alias("session_start"),
        F.max("enrolled_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.round((F.unix_timestamp(F.max("enrolled_at")) - F.unix_timestamp(F.min("enrolled_at"))) / 60, 1).alias("duration_min"),
    )
)

with_activity = (
    session_summary
    .crossJoin(session_summary.agg(F.max("event_count").alias("overall_max_event_count")))
    .filter(F.col("event_count") < F.col("overall_max_event_count"))
    .drop("overall_max_event_count")
)

sequenced = (
    with_activity
    .select(
        "*",
        F.lead("session_end", 3).over(w_all_by_duration_min).alias("next3_session_end"),
        F.max("duration_min").over(w_grade_status_session_end_mon).alias("grade_status_session_end_month_max_duration_min"),
        F.min("event_count").over(w_grade_status_session_end_mon).alias("grade_status_session_end_month_min_event_count"),
        F.round(F.col("session_id") - F.avg("session_id").over(w_grade_status_session_end_mon), 2).alias("session_id_vs_grade_status_session_end_month_avg"),
    )
)

result = (
    sequenced
    .select(
        "grade_status",
        "session_id",
        "session_start",
        "session_end",
        "event_count",
        "duration_min",
        "next3_session_end",
        "grade_status_session_end_month_min_event_count",
        F.when(F.col("event_count") >= 4, "HIGH").when(F.col("event_count") >= 3, "MEDIUM").otherwise("LOW").alias("event_count_tier"),
        F.when(F.col("session_id") >= 4, "A").when(F.col("session_id") >= 2, "B").otherwise("C").alias("session_id_tier"),
    )
    .orderBy(F.col("grade_status").desc())
)
