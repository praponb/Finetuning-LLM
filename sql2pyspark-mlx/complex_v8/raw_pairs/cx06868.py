from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term = Window.partitionBy("term").orderBy("enrolled_at", "id")
w_term_running = Window.partitionBy("term").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

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

with_prev = base.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_term))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_enrolled_at").isNull() | ((F.unix_timestamp("enrolled_at") - F.unix_timestamp("prev_enrolled_at")) / 60 > 120), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("starts_session").over(w_term_running))

filtered = (
    sessionized.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.term").isin("2024T1", "2024T2")) & (F.hour(F.col("e.enrolled_at")).between(6, 12)), "left_anti")
)

with_details_course = (
    filtered.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .filter(F.col("c.dept") == "Biz")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.prev_enrolled_at",
        "s.starts_session",
        "s.session_no",
        F.col("c.dept").alias("course_dept"),
        F.col("st.cohort").alias("student_cohort"),
    )
)

result = (
    with_details_course
    .select("id", "starts_session", "session_no")
    .unpivot(
        ids=["id"],
        values=["starts_session", "session_no"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("id", "metric_name")
)
