from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status = Window.partitionBy("grade_status").orderBy("enrolled_at", "id")
w_grade_status_running = Window.partitionBy("grade_status").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_enrollments = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

with_prev = scoped_enrollments.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_grade_status))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_enrolled_at").isNull() | ((F.unix_timestamp("enrolled_at") - F.unix_timestamp("prev_enrolled_at")) / 60 > 90), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_grade_status_running))

session_summary = (
    sessionized
    .groupBy("grade_status", "session_id")
    .agg(
        F.min("enrolled_at").alias("session_start"),
        F.max("enrolled_at").alias("session_end"),
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("enrolled_at")) - F.unix_timestamp(F.min("enrolled_at"))) / 60, 1).alias("duration_min"),
    )
)

grouped = (
    session_summary
    .groupBy(
        "session_id",
        F.date_format("session_end", "yyyy-MM").alias("session_end_ym"),
    )
    .agg(
        F.round(F.sum("session_id") / F.nullif(F.sum("duration_min"), F.lit(0)), 3).alias("session_id_to_duration_min_ratio"),
        F.sum("session_id").alias("total_session_id"),
        F.countDistinct("grade_status").alias("distinct_grade_status"),
        F.max("session_start").alias("last_session_start"),
    )
)

result = (
    grouped
    .filter(F.col("session_id") >= 2)
    .select(
        "session_id",
        "session_end_ym",
        "session_id_to_duration_min_ratio",
        "last_session_start",
    )
    .orderBy(F.col("session_id_to_duration_min_ratio").desc())
)
