from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id = Window.partitionBy("course_id").orderBy("enrolled_at", "id")
w_course_id_running = Window.partitionBy("course_id").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_new_session_flag = Window.partitionBy("new_session_flag")
w_student_id_by_new_session_fl = Window.partitionBy("student_id").orderBy("new_session_flag", "id")

base_enrollments = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
    )
)

with_prev = base_enrollments.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_course_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_enrolled_at").isNull() | (F.col("enrolled_at") > F.col("prev_enrolled_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_course_id_running))

sequenced = (
    sessionized
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "prev_enrolled_at",
        "new_session_flag",
        "session_no",
        F.round(F.col("new_session_flag") - F.avg("new_session_flag").over(w_new_session_flag), 2).alias("new_session_flag_vs_new_session_flag_avg"),
        F.round((F.unix_timestamp("prev_enrolled_at") - F.unix_timestamp(F.lag("prev_enrolled_at").over(w_student_id_by_new_session_fl))) / 60, 1).alias("minutes_since_prev_prev_enrolled_at"),
    )
)

joined_student = (
    sequenced.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .filter(F.col("st.cohort") != "C2024")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.prev_enrolled_at",
        "s.new_session_flag",
        "s.session_no",
        "s.new_session_flag_vs_new_session_flag_avg",
        "s.minutes_since_prev_prev_enrolled_at",
        F.col("st.cohort").alias("student_cohort"),
    )
)

result = (
    joined_student
    .filter(F.col("session_no") < 4)
    .select(
        "id",
        "student_id",
        "term",
        "grade_status",
        "prev_enrolled_at",
        "session_no",
        "new_session_flag_vs_new_session_flag_avg",
        "student_cohort",
    )
    .orderBy(F.col("grade_status").asc_nulls_last())
)
