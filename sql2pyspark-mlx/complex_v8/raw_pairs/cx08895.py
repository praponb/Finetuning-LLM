from pyspark.sql import functions as F

recent = (
    df_courses
    .filter(F.col("credits") >= 2)
    .select(
        "id",
        "course_code",
        "dept",
        "level",
        F.lower(F.regexp_replace(F.trim(F.col("course_code")), " ", "_")).alias("course_code_slug"),
        F.upper(F.trim(F.col("course_code"))).alias("course_code_clean"),
        F.concat_ws(" / ", F.col("course_code"), F.col("dept")).alias("course_code_dept_label"),
    )
)

exploded_detail_enrollments = (
    recent.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.course_code_slug",
        "s.course_code_dept_label",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.student_id").alias("enrollment_student_id"),
        F.col("e.score").alias("enrollment_score"),
    )
)

result = (
    exploded_detail_enrollments
    .groupingSets(
        [["enrollment_student_id", "enrollment_id"], ["enrollment_student_id"], ["enrollment_id"], []],
        "enrollment_student_id", "enrollment_id",
    )
    .agg(
        F.grouping("enrollment_student_id").alias("is_total_enrollment_student_id"),
        F.countDistinct("enrollment_id").alias("distinct_enrollment_ids"),
        F.max("enrollment_score").alias("max_enrollment_score"),
    )
)
