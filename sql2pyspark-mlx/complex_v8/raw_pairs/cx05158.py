from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id = Window.partitionBy("student_id").orderBy("enrolled_at", "id")
w_student_id_running = Window.partitionBy("student_id").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_student_id_2 = Window.partitionBy("student_id")
w_student_id_by_session_end = Window.partitionBy("student_id").orderBy(F.col("session_end").desc(), "student_id", "burst_id")
w_burst_id_by_duration_min = Window.partitionBy("burst_id").orderBy("duration_min", "student_id", "burst_id")

scoped = (
    df_enrollments
    .filter(F.col("grade_status") == "PASS")
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
    )
)

with_prev = scoped.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_student_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_enrolled_at").isNull() | ((F.unix_timestamp("enrolled_at") - F.unix_timestamp("prev_enrolled_at")) / 60 > 120), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("new_session_flag").over(w_student_id_running))

session_summary = (
    sessionized
    .groupBy("student_id", "burst_id")
    .agg(
        F.min("enrolled_at").alias("session_start"),
        F.max("enrolled_at").alias("session_end"),
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("enrolled_at")) - F.unix_timestamp(F.min("enrolled_at"))) / 60, 1).alias("duration_min"),
    )
)

metrics = (
    session_summary
    .select(
        "student_id",
        "burst_id",
        "session_start",
        "session_end",
        "events",
        "duration_min",
        F.count(F.lit(1)).over(w_student_id_2).alias("rows_in_student_id"),
        F.lead("events", 2).over(w_student_id_by_session_end).alias("next2_events"),
        F.max("events").over(w_student_id_2).alias("student_id_max_events"),
        (F.col("events") - F.lag("events").over(w_burst_id_by_duration_min)).alias("events_change"),
    )
)

result = (
    metrics
    .select(
        "student_id",
        "burst_id",
        "session_end",
        "duration_min",
        "next2_events",
        "student_id_max_events",
        "events_change",
    )
    .orderBy("next2_events", F.col("student_id").desc(), "burst_id")
    .limit(10)
)
