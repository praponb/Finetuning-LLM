from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id = Window.partitionBy("course_id").orderBy("enrolled_at", "id")
w_course_id_running = Window.partitionBy("course_id").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_enrollments
    .filter(F.col("course_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.substring(F.trim(F.col("term")), 1, 3).alias("term_prefix"),
    )
)

with_prev = base.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_course_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_enrolled_at").isNull() | ((F.unix_timestamp("enrolled_at") - F.unix_timestamp("prev_enrolled_at")) / 60 > 30), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w_course_id_running))

sessions = (
    sessionized
    .groupBy("course_id", "session_no")
    .agg(
        F.min("enrolled_at").alias("session_start"),
        F.max("enrolled_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sum("score").alias("session_score"),
        F.round((F.unix_timestamp(F.max("enrolled_at")) - F.unix_timestamp(F.min("enrolled_at"))) / 60, 1).alias("duration_min"),
    )
)

result = (
    sessions
    .select(
        "course_id",
        "session_no",
        "session_start",
        "session_end",
        "event_count",
        "session_score",
        "duration_min",
        F.year(F.col("session_end")).alias("session_end_year"),
        F.when(F.col("session_no") >= 4, "A").when(F.col("session_no") >= 2, "B").otherwise("C").alias("session_no_tier"),
    )
    .orderBy(
        F.col("session_no").desc_nulls_first(),
        F.col("duration_min").desc(),
        "course_id",
    )
    .limit(10)
)
