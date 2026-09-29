from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("term", "student_id").orderBy(F.col("enrolled_at").desc(), F.col("id").desc())

base_enrollments = df_enrollments.filter(F.col("student_id") >= 17)

deduped = (
    base_enrollments
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

combined_details_student = (
    deduped.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status",
        "s.enrolled_at",
        F.col("st.major").alias("student_major"),
        F.col("c.dept").alias("course_dept"),
    )
)

joined_student = (
    combined_details_student.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.*",
        "st.student_name",
        F.col("st.cohort").alias("student_cohort"),
        F.col("st.enrolled_on").alias("student_enrolled_on"),
    )
)

line_level = (
    joined_student.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status",
        "s.enrolled_at",
        "s.student_name",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
    )
)

filtered = (
    line_level
    .crossJoin(line_level.agg(F.avg("enrollment_score").alias("overall_avg_enrollment_score")))
    .filter(F.col("enrollment_score") > F.col("overall_avg_enrollment_score"))
    .drop("overall_avg_enrollment_score")
)

result = (
    filtered
    .select(
        "id",
        "course_id",
        "enrolled_at",
        "student_name",
        "enrollment_id",
        "enrollment_enrolled_at",
        "enrollment_score",
        "enrollment_grade_status",
        F.substring(F.trim(F.col("enrollment_grade_status")), 1, 4).alias("enrollment_grade_status_prefix"),
    )
    .orderBy(F.col("enrollment_id").desc(), "id")
    .limit(10)
)
