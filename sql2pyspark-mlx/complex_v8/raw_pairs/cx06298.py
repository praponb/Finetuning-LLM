from pyspark.sql import functions as F

recent = (
    df_enrollments
    .filter(F.col("student_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.date_format(F.col("enrolled_at"), "MMM yyyy").alias("enrolled_at_label"),
    )
)

joined_student = (
    recent.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.*",
        F.col("st.major").alias("student_major"),
        F.col("c.level").alias("course_level"),
        "c.course_code",
    )
)

with_details_course = (
    joined_student.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .filter(F.col("c.dept").isNotNull())
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.enrolled_at",
        "s.enrolled_at_label",
        "s.student_major",
        "s.course_level",
        F.col("c.dept").alias("course_dept"),
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        "st.student_name",
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(F.count("*").alias("enrollment_count"))
)

profiled = (
    with_details_course.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

result = (
    profiled
    .cube("student_id", "course_id", "enrollment_count")
    .agg(
        F.grouping("student_id").alias("is_total_student_id"),
        F.array_sort(F.collect_set("course_dept")).alias("course_dept_list"),
        F.round(F.avg("score"), 2).alias("avg_score"),
    )
)
