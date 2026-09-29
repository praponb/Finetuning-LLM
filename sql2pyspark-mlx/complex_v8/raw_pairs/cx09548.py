from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id = Window.partitionBy("course_id").orderBy("enrolled_at", "id")
w_course_id_running = Window.partitionBy("course_id").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
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

with_prev = base.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_course_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_enrolled_at").isNull() | ((F.unix_timestamp("enrolled_at") - F.unix_timestamp("prev_enrolled_at")) / 60 > 120), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("starts_session").over(w_course_id_running))

with_details_student = (
    sessionized.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .filter(F.col("st.gpa") > 2.5)
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.enrolled_at",
        "s.prev_enrolled_at",
        "s.starts_session",
        "s.session_no",
        F.col("st.major").alias("student_major"),
        F.col("st.is_international").alias("student_is_international"),
        F.col("st.gpa").alias("student_gpa"),
    )
)

scored = (
    with_details_student
    .select(
        "*",
        F.when(F.col("starts_session") >= 1, "A").when(F.col("starts_session") >= 0, "B").otherwise("C").alias("starts_session_tier"),
        F.abs(F.col("starts_session") - 0).alias("starts_session_distance"),
        F.dayofweek(F.col("enrolled_at")).alias("enrolled_at_dayofweek"),
    )
)

result = (
    scored
    .filter(F.col("student_major") == "Bio")
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        "prev_enrolled_at",
        "session_no",
        "student_major",
        "starts_session_distance",
        "enrolled_at_dayofweek",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
    )
    .orderBy(F.col("prev_enrolled_at").desc(), "id")
    .limit(5)
)
