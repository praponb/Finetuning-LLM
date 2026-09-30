from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id = Window.partitionBy("student_id").orderBy("enrolled_at_month", "id")
w_student_id_running = Window.partitionBy("student_id").orderBy("enrolled_at_month", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_enrollments = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.date_trunc("month", F.col("enrolled_at")).alias("enrolled_at_month"),
    )
)

with_prev = scoped_enrollments.withColumn("prev_enrolled_at_month", F.lag("enrolled_at_month").over(w_student_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_enrolled_at_month").isNull() | (F.col("enrolled_at_month") > F.col("prev_enrolled_at_month") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_student_id_running))

session_summary = (
    sessionized
    .groupBy("student_id", "session_no")
    .agg(
        F.min("enrolled_at_month").alias("session_start"),
        F.max("enrolled_at_month").alias("session_end"),
        F.count("*").alias("events"),
        F.sum("score").alias("session_score"),
        F.round((F.unix_timestamp(F.max("enrolled_at_month")) - F.unix_timestamp(F.min("enrolled_at_month"))) / 60, 1).alias("duration_min"),
    )
)

detail_rows = (
    session_summary.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.term") != "2025T2"), "inner")
    .select(
        "s.student_id",
        "s.session_no",
        "s.session_end",
        "s.events",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.course_id").alias("enrollment_course_id"),
        F.col("e.score").alias("enrollment_score"),
    )
)

enriched = (
    detail_rows
    .select(
        "student_id",
        "session_no",
        "session_end",
        "enrollment_id",
        "enrollment_course_id",
        F.greatest(F.col("enrollment_score"), F.col("session_no")).alias("greatest_enrollment_score_session_no"),
        F.concat(F.col("enrollment_term"), F.lit("#"), F.col("session_no").cast("string")).alias("enrollment_term_ref"),
    )
)

result = (
    enriched
    .select(
        "student_id",
        "session_no",
        "session_end",
        "enrollment_id",
        "enrollment_course_id",
        "greatest_enrollment_score_session_no",
        "enrollment_term_ref",
        F.upper(F.trim(F.col("enrollment_term_ref"))).alias("enrollment_term_ref_clean"),
        F.substring(F.trim(F.col("enrollment_term_ref")), 1, 4).alias("enrollment_term_ref_prefix"),
    )
    .orderBy(
        F.col("enrollment_term_ref_clean").asc(),
        F.col("greatest_enrollment_score_session_no").desc_nulls_first(),
    )
)
