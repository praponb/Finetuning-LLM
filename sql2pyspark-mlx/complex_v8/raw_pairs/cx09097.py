from pyspark.sql import functions as F

base_courses = (
    df_courses
    .filter((F.col("credits") <= 3) & (F.col("level") == "core"))
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
        F.length(F.trim(F.col("level"))).alias("level_len"),
        F.length(F.trim(F.col("dept"))).alias("dept_len"),
    )
)

derived = (
    base_courses
    .select(
        "*",
        F.length(F.trim(F.col("level"))).alias("level_len_2"),
        (F.col("level_len") + F.col("credits")).alias("level_len_plus_credits"),
        F.concat_ws("|", F.col("level"), F.col("dept")).alias("level_dept_label"),
    )
)

detail_rows_enrollments = (
    derived.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.id")) & (~F.col("e.grade_status").isin("WITHDRAWN", "PASS")), "inner")
    .select(
        "s.id",
        "s.dept",
        "s.level_len_2",
        "s.level_len_plus_credits",
        "s.level_dept_label",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.student_id").alias("enrollment_student_id"),
    )
)

result = (
    detail_rows_enrollments
    .filter(F.col("dept").isin("CS", "Art"))
    .select(
        "id",
        "dept",
        "level_len_2",
        "level_len_plus_credits",
        "level_dept_label",
        "enrollment_id",
        "enrollment_grade_status",
        "enrollment_term",
        "enrollment_student_id",
        F.abs(F.col("level_len_2") - 7).alias("level_len_2_distance"),
        F.substring(F.trim(F.col("level_dept_label")), 1, 4).alias("level_dept_label_prefix"),
    )
)
