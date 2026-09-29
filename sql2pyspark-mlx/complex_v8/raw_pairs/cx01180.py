from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id = Window.partitionBy("student_id").orderBy("enrolled_at", "id")
w_student_id_running = Window.partitionBy("student_id").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = df_enrollments.filter(F.col("term") == "2024T1")

with_prev = base.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_student_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_enrolled_at").isNull() | ((F.unix_timestamp("enrolled_at") - F.unix_timestamp("prev_enrolled_at")) / 60 > 90), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_student_id_running))

per_session = (
    sessionized
    .groupBy("student_id", "session_no")
    .agg(
        F.min("enrolled_at").alias("session_start"),
        F.max("enrolled_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sum("score").alias("session_score"),
        F.round((F.unix_timestamp(F.max("enrolled_at")) - F.unix_timestamp(F.min("enrolled_at"))) / 60, 1).alias("duration_min"),
    )
)

enriched = (
    per_session.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.student_id",
        "s.session_no",
        "s.session_score",
        "s.duration_min",
        F.col("st.enrolled_on").alias("student_enrolled_on"),
    )
)

detail_rows = (
    enriched.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.student_id",
        "s.session_no",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.course_id").alias("enrollment_course_id"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
    )
)

result = (
    detail_rows
    .select(
        "student_id",
        "session_no",
        "enrollment_id",
        "enrollment_course_id",
        "enrollment_enrolled_at",
    )
    .orderBy(F.col("enrollment_id").asc_nulls_last(), "student_id", "session_no")
    .limit(20)
)
