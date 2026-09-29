from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id = Window.partitionBy("student_id").orderBy("enrolled_at", "id")
w_student_id_running = Window.partitionBy("student_id").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

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

with_prev = recent_enrollments.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_student_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_enrolled_at").isNull() | ((F.unix_timestamp("enrolled_at") - F.unix_timestamp("prev_enrolled_at")) / 60 > 120), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("new_session_flag").over(w_student_id_running))

sessions = (
    sessionized
    .groupBy("student_id", "burst_id")
    .agg(
        F.min("enrolled_at").alias("session_start"),
        F.max("enrolled_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.round((F.unix_timestamp(F.max("enrolled_at")) - F.unix_timestamp(F.min("enrolled_at"))) / 60, 1).alias("duration_min"),
    )
)

filtered = (
    sessions.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.hour(F.col("e.enrolled_at")).between(6, 10)), "left_anti")
)

result = (
    filtered
    .filter(F.col("burst_id").between(2, 5))
    .select(
        "student_id",
        "burst_id",
        "session_start",
        "session_end",
        "n_events",
    )
    .orderBy(F.col("burst_id").desc())
)
