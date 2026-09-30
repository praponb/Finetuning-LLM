from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term = Window.partitionBy("term").orderBy("enrolled_at", "id")
w_term_running = Window.partitionBy("term").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent = (
    df_enrollments
    .filter(F.col("term").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.lower(F.regexp_replace(F.trim(F.col("term")), " ", "_")).alias("term_slug"),
    )
)

with_prev = recent.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_term))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_enrolled_at").isNull() | ((F.unix_timestamp("enrolled_at") - F.unix_timestamp("prev_enrolled_at")) / 60 > 60), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_term_running))

with_activity = (
    sessionized
    .crossJoin(sessionized.agg(F.avg("session_no").alias("overall_avg_session_no")))
    .filter(F.col("session_no") > F.col("overall_avg_session_no"))
    .drop("overall_avg_session_no")
)

enriched = (
    with_activity.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (F.col("st.enrolled_on").isNotNull()), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.enrolled_at",
        "s.term_slug",
        "s.prev_enrolled_at",
        "s.new_session_flag",
        "s.session_no",
        "st.student_name",
        F.col("st.cohort").alias("student_cohort"),
        F.col("st.major").alias("student_major"),
    )
)

result = (
    enriched
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "enrolled_at",
        "term_slug",
        "prev_enrolled_at",
        "new_session_flag",
        "session_no",
        "student_major",
        F.when(F.col("session_no") >= 4, "A").when(F.col("session_no") >= 2, "B").otherwise("C").alias("session_no_tier"),
        F.date_trunc("week", F.col("prev_enrolled_at")).alias("prev_enrolled_at_week"),
    )
    .orderBy(F.col("session_no_tier").asc_nulls_last(), "prev_enrolled_at")
)
