from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id = Window.partitionBy("course_id").orderBy("enrolled_at", "id")
w_course_id_running = Window.partitionBy("course_id").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_is_new_session_moving = Window.orderBy(F.col("is_new_session").desc(), "id").rowsBetween(-1, Window.currentRow)
w_grade_status_enrolled_at_mon = Window.partitionBy("grade_status", F.date_trunc("month", "enrolled_at"))

base_enrollments = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.date_trunc("hour", F.col("enrolled_at")).alias("enrolled_at_hour"),
    )
)

with_prev = base_enrollments.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_course_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_enrolled_at").isNull() | (F.col("enrolled_at") > F.col("prev_enrolled_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_course_id_running))

ranked = (
    sessionized
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        "prev_enrolled_at",
        "is_new_session",
        "session_id",
        F.sum("session_id").over(w_all_by_is_new_session_moving).alias("session_id_sum_2"),
        F.max("session_id").over(w_grade_status_enrolled_at_mon).alias("grade_status_enrolled_at_month_max_session_id"),
    )
)

enriched = (
    ranked.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status",
        "s.enrolled_at",
        "s.prev_enrolled_at",
        "s.is_new_session",
        "s.session_id",
        "s.session_id_sum_2",
        "s.grade_status_enrolled_at_month_max_session_id",
        F.col("c.dept").alias("course_dept"),
        F.col("st.gpa").alias("student_gpa"),
    )
)

result = (
    enriched
    .select(
        "id",
        "student_id",
        "course_id",
        "grade_status",
        "enrolled_at",
        "prev_enrolled_at",
        "is_new_session",
        "session_id",
        "session_id_sum_2",
        "grade_status_enrolled_at_month_max_session_id",
        "course_dept",
        "student_gpa",
    )
    .orderBy(F.col("course_id").desc(), F.col("id").asc_nulls_last())
)
