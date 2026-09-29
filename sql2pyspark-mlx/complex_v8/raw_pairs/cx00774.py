from pyspark.sql import functions as F

recent_students = (
    df_students
    .filter(F.col("gpa") >= 2.0)
    .select(
        "id",
        "student_name",
        "enrolled_on",
        "gpa",
        F.last_day(F.col("enrolled_on")).alias("enrolled_on_month_end"),
    )
)

exploded_detail_enrollments = (
    recent_students.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.enrolled_on",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.course_id").alias("enrollment_course_id"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
    )
)

screened = (
    exploded_detail_enrollments.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.id")) & (F.col("e.score") >= 36.6) & (F.col("e.term") != "2025T1"), "left_anti")
    .filter(F.col("enrollment_course_id") <= 3)
)

enriched = (
    screened
    .select(
        "id",
        "enrolled_on",
        "enrollment_id",
        "enrollment_grade_status",
        F.last_day(F.col("enrolled_on")).alias("enrolled_on_month_end"),
        F.lower(F.regexp_replace(F.trim(F.col("enrollment_grade_status")), " ", "_")).alias("enrollment_grade_status_slug"),
    )
)

result = (
    enriched
    .filter(F.year(F.col("enrolled_on_month_end")) == 2025)
    .select(
        "id",
        "enrollment_id",
        "enrollment_grade_status",
        "enrolled_on_month_end",
        "enrollment_grade_status_slug",
        F.date_format(F.col("enrolled_on"), "yyyy-MM").alias("enrolled_on_label"),
        F.lower(F.trim(F.col("enrollment_grade_status"))).alias("enrollment_grade_status_clean"),
    )
    .orderBy("enrolled_on_label", "id", "enrollment_id")
    .limit(20)
)
