from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_by_enrolled_at = Window.partitionBy("grade_status").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grade_status_by_enrolled_at_2 = Window.partitionBy("grade_status").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_grade_status_by_enrolled_at_3 = Window.partitionBy("grade_status").orderBy(F.col("enrolled_at").desc())
w_grade_status_by_enrolled_at_4 = Window.partitionBy("grade_status").orderBy(F.col("enrolled_at").desc(), "id")

base = (
    df_enrollments
    .filter(F.col("grade_status") == "INCOMPLETE")
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
    )
)

filtered = (
    base.alias("s")
    .join(df_courses.alias("c"), (F.col("c.id") == F.col("s.course_id")) & (F.col("c.credits") >= 2) & (F.col("c.course_code").isNotNull()), "left_semi")
)

windowed = (
    filtered
    .select(
        "*",
        F.first("enrolled_at", ignorenulls=True).over(w_grade_status_by_enrolled_at).alias("first_known_enrolled_at"),
        F.last("enrolled_at").over(w_grade_status_by_enrolled_at_2).alias("last_enrolled_at"),
        F.round(F.cume_dist().over(w_grade_status_by_enrolled_at_3), 3).alias("enrolled_at_cume_dist"),
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_grade_status_by_enrolled_at_4))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
        F.dense_rank().over(w_grade_status_by_enrolled_at_3).alias("enrolled_at_dense_rank"),
        F.last("enrolled_at", ignorenulls=True).over(w_grade_status_by_enrolled_at).alias("last_known_enrolled_at"),
    )
)

leaders = windowed.filter(F.col("enrolled_at_dense_rank") <= 2)

combined_details = (
    leaders.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.first_known_enrolled_at",
        "s.last_enrolled_at",
        "s.minutes_since_prev_enrolled_at",
        F.col("st.gpa").alias("student_gpa"),
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        F.col("st.major").alias("student_major"),
    )
)

summary = (
    combined_details
    .filter(F.col("student_gpa") >= 2.7)
    .groupBy(
        F.to_date("first_known_enrolled_at").alias("first_known_enrolled_at_day"),
    )
    .agg(
        F.countDistinct("course_id").alias("distinct_course_ids"),
        F.round(F.avg("minutes_since_prev_enrolled_at"), 2).alias("avg_minutes_since_prev_enrolled_at"),
        F.min("minutes_since_prev_enrolled_at").alias("min_minutes_since_prev_enrolled_at"),
        F.sum(F.when(F.col("term") == "2025T1", F.col("minutes_since_prev_enrolled_at"))).alias("minutes_since_prev_enrolled_at_term_2025t1"),
        F.min("student_enrolled_on").alias("first_student_enrolled_on"),
        F.round(F.sum("student_gpa"), 2).alias("total_student_gpa"),
    )
)

result = (
    summary
    .select(
        "first_known_enrolled_at_day",
        "distinct_course_ids",
        "avg_minutes_since_prev_enrolled_at",
        "minutes_since_prev_enrolled_at_term_2025t1",
        "first_student_enrolled_on",
        (F.col("distinct_course_ids") + F.col("total_student_gpa")).alias("distinct_course_ids_plus_total_student_gpa"),
    )
    .orderBy(F.col("first_student_enrolled_on").asc())
)
