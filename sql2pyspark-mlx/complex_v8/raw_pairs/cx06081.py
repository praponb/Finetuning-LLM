from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id = Window.partitionBy("course_id").orderBy("enrolled_at", "id")
w_course_id_running = Window.partitionBy("course_id").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_count_by_session_start = Window.partitionBy("event_count").orderBy("session_start", "course_id", "burst_id")
w_event_count_by_session_start_2 = Window.partitionBy("event_count").orderBy("session_start", "course_id", "burst_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_burst_id_event_count_by_sess = Window.partitionBy("burst_id", "event_count").orderBy("session_end", "course_id", "burst_id")
w_session_end_month = Window.partitionBy(F.date_trunc("month", "session_end"))

prepared = (
    df_enrollments
    .filter(F.col("term") == "2025T1")
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
    )
)

with_prev = prepared.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_course_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_enrolled_at").isNull() | ((F.unix_timestamp("enrolled_at") - F.unix_timestamp("prev_enrolled_at")) / 60 > 90), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_course_id_running))

session_summary = (
    sessionized
    .groupBy("course_id", "burst_id")
    .agg(
        F.min("enrolled_at").alias("session_start"),
        F.max("enrolled_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.round((F.unix_timestamp(F.max("enrolled_at")) - F.unix_timestamp(F.min("enrolled_at"))) / 60, 1).alias("duration_min"),
    )
)

with_windows = (
    session_summary
    .select(
        "course_id",
        "burst_id",
        "session_start",
        "event_count",
        F.row_number().over(w_event_count_by_session_start).alias("seq_no"),
        F.last("duration_min").over(w_event_count_by_session_start_2).alias("last_duration_min"),
        F.lead("session_end", 2).over(w_burst_id_event_count_by_sess).alias("next2_session_end"),
        F.lead("session_end").over(w_burst_id_event_count_by_sess).alias("next_session_end"),
        F.count(F.lit(1)).over(w_session_end_month).alias("rows_in_session_end_month"),
    )
)

result = (
    with_windows
    .select(
        "course_id",
        "burst_id",
        "session_start",
        "seq_no",
        "last_duration_min",
        "next2_session_end",
        "next_session_end",
        "rows_in_session_end_month",
        F.round(F.col("last_duration_min") * 1.1, 2).alias("last_duration_min_adj"),
        F.greatest(F.col("seq_no"), F.col("event_count")).alias("greatest_seq_no_event_count"),
    )
    .orderBy(F.col("next_session_end").desc())
)
