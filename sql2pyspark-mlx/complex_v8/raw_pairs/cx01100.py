from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id = Window.partitionBy("course_id").orderBy("enrolled_at", "id")
w_course_id_running = Window.partitionBy("course_id").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared_enrollments = (
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

with_prev = prepared_enrollments.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_course_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_enrolled_at").isNull() | ((F.unix_timestamp("enrolled_at") - F.unix_timestamp("prev_enrolled_at")) / 60 > 30), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_course_id_running))

per_session = (
    sessionized
    .groupBy("course_id", "burst_id")
    .agg(
        F.min("enrolled_at").alias("session_start"),
        F.max("enrolled_at").alias("session_end"),
        F.count("*").alias("events"),
        F.sum("score").alias("session_score"),
        F.round((F.unix_timestamp(F.max("enrolled_at")) - F.unix_timestamp(F.min("enrolled_at"))) / 60, 1).alias("duration_min"),
    )
)

exploded_detail = (
    per_session.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.course_id",
        "s.burst_id",
        "s.session_end",
        "s.session_score",
        "s.duration_min",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
    )
)

labeled = (
    exploded_detail
    .select(
        "course_id",
        "burst_id",
        "session_end",
        "session_score",
        "duration_min",
        "enrollment_id",
        "enrollment_term",
        "enrollment_score",
        "enrollment_grade_status",
        F.date_trunc("quarter", F.col("session_end")).alias("session_end_quarter"),
    )
)

result = (
    labeled
    .select(
        "course_id",
        "burst_id",
        "session_end",
        "session_score",
        "duration_min",
        "enrollment_id",
        "enrollment_term",
        "enrollment_grade_status",
        "session_end_quarter",
        F.when(F.col("enrollment_term").isin("2025T1", "2024T2"), "group_a").when(F.col("enrollment_term") == "2025T2", "group_b").otherwise("other").alias("enrollment_term_group"),
    )
    .orderBy(F.col("burst_id").desc())
)
