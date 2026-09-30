from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id = Window.partitionBy("course_id").orderBy("enrolled_at", "id")
w_course_id_running = Window.partitionBy("course_id").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_enrollments
    .filter(F.col("enrolled_at") >= F.lit("2025-02-06 11:00:00").cast("timestamp"))
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

with_prev = filtered.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_course_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_enrolled_at").isNull() | ((F.unix_timestamp("enrolled_at") - F.unix_timestamp("prev_enrolled_at")) / 60 > 60), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_course_id_running))

per_session = (
    sessionized
    .groupBy("course_id", "burst_id")
    .agg(
        F.min("enrolled_at").alias("session_start"),
        F.max("enrolled_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.round((F.unix_timestamp(F.max("enrolled_at")) - F.unix_timestamp(F.min("enrolled_at"))) / 60, 1).alias("duration_min"),
    )
)

with_details_course = (
    per_session.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select("s.*", "c.course_code", F.col("c.dept").alias("course_dept"))
)

filtered_2 = (
    with_details_course.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.student_id").isNotNull()) & (F.to_date(F.col("e.enrolled_at")) < F.lit("2025-03-08").cast("date")), "left_semi")
)

result = (
    filtered_2
    .select(
        "course_id",
        "session_start",
        "session_end",
        "n_events",
        "duration_min",
        "course_code",
        F.year(F.col("session_end")).alias("session_end_year"),
        F.length(F.trim(F.col("course_code"))).alias("course_code_len"),
    )
    .orderBy(F.col("n_events").desc(), F.col("duration_min").desc())
)
