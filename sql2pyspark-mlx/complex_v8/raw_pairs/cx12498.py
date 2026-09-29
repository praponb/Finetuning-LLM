from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id = Window.partitionBy("course_id").orderBy("enrolled_at", "id")
w_course_id_running = Window.partitionBy("course_id").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.initcap(F.trim(F.col("term"))).alias("term_clean"),
    )
)

with_prev = cleaned.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_course_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_enrolled_at").isNull() | ((F.unix_timestamp("enrolled_at") - F.unix_timestamp("prev_enrolled_at")) / 60 > 120), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_course_id_running))

session_summary = (
    sessionized
    .groupBy("course_id", "session_id")
    .agg(
        F.min("enrolled_at").alias("session_start"),
        F.max("enrolled_at").alias("session_end"),
        F.count("*").alias("events"),
        F.sum("score").alias("session_score"),
        F.round((F.unix_timestamp(F.max("enrolled_at")) - F.unix_timestamp(F.min("enrolled_at"))) / 60, 1).alias("duration_min"),
    )
)

result = (
    session_summary
    .select(
        "course_id",
        "session_id",
        "session_start",
        "session_end",
        "events",
        "duration_min",
        F.round(F.col("session_score") * 0.9, 2).alias("session_score_adj"),
        F.greatest(F.col("session_id"), F.col("session_score")).alias("greatest_session_id_session_score"),
    )
    .orderBy(
        "duration_min",
        F.col("events").desc(),
        "course_id",
        "session_id",
    )
    .limit(10)
)
