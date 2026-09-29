from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status = Window.partitionBy("grade_status").orderBy("enrolled_at", "id")
w_grade_status_running = Window.partitionBy("grade_status").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_burst_id = Window.partitionBy("burst_id")
w_session_end_month_by_duratio = Window.partitionBy(F.month("session_end")).orderBy("duration_min", "grade_status", "burst_id")
w_all_by_duration_min_minus_n = Window.orderBy("duration_min_minus_n_events", "grade_status", "burst_id")

base = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.lower(F.trim(F.col("grade_status"))).alias("grade_status_clean"),
    )
)

with_prev = base.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_grade_status))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_enrolled_at").isNull() | (F.col("enrolled_at") > F.col("prev_enrolled_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("new_session_flag").over(w_grade_status_running))

sessions = (
    sessionized
    .groupBy("grade_status", "burst_id")
    .agg(
        F.min("enrolled_at").alias("session_start"),
        F.max("enrolled_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.round((F.unix_timestamp(F.max("enrolled_at")) - F.unix_timestamp(F.min("enrolled_at"))) / 60, 1).alias("duration_min"),
    )
)

enriched = (
    sessions
    .select(
        "grade_status",
        "burst_id",
        "session_start",
        "session_end",
        "n_events",
        "duration_min",
        F.date_trunc("hour", F.col("session_end")).alias("session_end_hour"),
        (F.col("duration_min") - F.col("n_events")).alias("duration_min_minus_n_events"),
    )
)

sequenced = (
    enriched
    .select(
        "*",
        F.round(F.col("duration_min") - F.avg("duration_min").over(w_burst_id), 2).alias("duration_min_vs_burst_id_avg"),
        F.lead("duration_min", 2).over(w_session_end_month_by_duratio).alias("next2_duration_min"),
        F.round((F.unix_timestamp("session_end_hour") - F.unix_timestamp(F.lag("session_end_hour").over(w_all_by_duration_min_minus_n))) / 60, 1).alias("minutes_since_prev_session_end_hour"),
        F.max("burst_id").over(w_burst_id).alias("burst_id_max_burst_id"),
    )
)

result = (
    sequenced
    .select(
        "grade_status",
        "burst_id",
        "duration_min",
        "minutes_since_prev_session_end_hour",
        "next2_duration_min",
        "duration_min_minus_n_events",
    )
    .unpivot(
        ids=["grade_status", "burst_id"],
        values=["duration_min", "minutes_since_prev_session_end_hour", "next2_duration_min", "duration_min_minus_n_events"],
        variableColumnName="measure",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("grade_status", "burst_id", "measure")
)
