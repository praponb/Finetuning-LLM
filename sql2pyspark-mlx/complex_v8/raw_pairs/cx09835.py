from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status = Window.partitionBy("grade_status").orderBy("enrolled_at", "id")
w_grade_status_running = Window.partitionBy("grade_status").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_count_by_duration_min = Window.partitionBy("event_count").orderBy("duration_min", "grade_status", "session_id")
w_event_count_by_duration_min_2 = Window.partitionBy("event_count").orderBy("duration_min", "grade_status", "session_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grade_status_session_id = Window.partitionBy("grade_status", "session_id")
w_session_id_last_known_sessio = Window.partitionBy("session_id", F.date_trunc("month", "last_known_session_start")).orderBy(F.col("event_count").desc())
w_session_end_month = Window.partitionBy(F.date_trunc("month", "session_end"))
w_session_id_last_known_sessio_2 = Window.partitionBy("session_id", F.date_trunc("month", "last_known_session_start")).orderBy(F.col("event_count").desc(), "grade_status", "session_id").rowsBetween(-2, -1)

filtered_enrollments = (
    df_enrollments
    .filter(F.col("student_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
    )
)

with_prev = filtered_enrollments.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_grade_status))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_enrolled_at").isNull() | (F.col("enrolled_at") > F.col("prev_enrolled_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("starts_session").over(w_grade_status_running))

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

with_trends = (
    session_summary
    .select(
        "*",
        F.lead("session_end").over(w_event_count_by_duration_min).alias("next_session_end"),
        F.rank().over(w_event_count_by_duration_min).alias("duration_min_rank"),
        F.lead("session_id", 2).over(w_event_count_by_duration_min).alias("next2_session_id"),
        F.row_number().over(w_event_count_by_duration_min).alias("rn"),
        F.last("session_start", ignorenulls=True).over(w_event_count_by_duration_min_2).alias("last_known_session_start"),
        F.round(F.col("duration_min") / F.nullif(F.sum("duration_min").over(w_grade_status_session_id), F.lit(0)), 4).alias("duration_min_share_of_grade_status_session_id"),
    )
)

sequenced = (
    with_trends
    .select(
        "*",
        F.rank().over(w_session_id_last_known_sessio).alias("event_count_rank"),
        F.count(F.lit(1)).over(w_session_end_month).alias("rows_in_session_end_month"),
        F.round(F.avg("duration_min").over(w_session_id_last_known_sessio_2), 2).alias("duration_min_avg_2_prior"),
        F.max("duration_min").over(w_session_end_month).alias("session_end_month_max_duration_min"),
    )
)

result = (
    sequenced
    .select(
        "grade_status",
        "session_id",
        "session_start",
        "event_count",
        "duration_min",
        "next_session_end",
        "duration_min_rank",
        "rn",
        "last_known_session_start",
        "duration_min_share_of_grade_status_session_id",
        "event_count_rank",
        "rows_in_session_end_month",
        "duration_min_avg_2_prior",
    )
    .orderBy(
        F.col("duration_min_share_of_grade_status_session_id").desc(),
        "session_start",
    )
)
