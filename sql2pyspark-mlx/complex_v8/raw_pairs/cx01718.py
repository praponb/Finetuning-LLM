from pyspark.sql import functions as F

valid_enrollments = (
    df_enrollments
    .filter(F.col("course_id") <= 6)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.concat_ws("-", F.col("term"), F.col("grade_status")).alias("term_grade_status_label"),
    )
)

tagged = (
    valid_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        "term_grade_status_label",
        F.lower(F.regexp_replace(F.trim(F.col("term_grade_status_label")), " ", "_")).alias("term_grade_status_label_slug"),
    )
)

with_children_enrollments = (
    tagged.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.student_id").alias("enrollment_student_id"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
    )
)

result = (
    with_children_enrollments
    .filter(F.col("enrollment_id").isNotNull())
    .select(
        "id",
        "course_id",
        "enrollment_student_id",
        "enrollment_term",
        F.substring(F.trim(F.col("enrollment_grade_status")), 1, 2).alias("enrollment_grade_status_prefix"),
    )
    .orderBy(F.col("course_id").desc())
)
