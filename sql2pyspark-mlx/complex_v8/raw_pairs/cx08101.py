from pyspark.sql import functions as F

cleaned = (
    df_enrollments
    .filter(F.col("term").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.quarter(F.col("enrolled_at")).alias("enrolled_at_quarter"),
    )
)

joined_course = (
    cleaned.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status",
        "s.enrolled_at",
        "s.enrolled_at_quarter",
        F.col("c.level").alias("course_level"),
        "c.course_code",
        F.col("c.dept").alias("course_dept"),
    )
)

annotated = (
    joined_course
    .select(
        "*",
        F.when(F.col("course_level").isin("intro", "advanced"), "group_a").when(F.col("course_level") == "core", "group_b").otherwise("other").alias("course_level_group"),
        F.concat_ws(" / ", F.col("grade_status"), F.col("course_dept")).alias("grade_status_course_dept_label"),
    )
)

with_parent_course = (
    annotated.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select("s.*", F.col("c.dept").alias("course_dept_2"))
)

labeled = (
    with_parent_course
    .select(
        "id",
        "student_id",
        "course_id",
        "grade_status",
        "enrolled_at_quarter",
        "course_level",
        "course_code",
        "course_dept",
        F.date_format(F.col("enrolled_at"), "yyyy-MM").alias("enrolled_at_label"),
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
    )
)

result = (
    labeled
    .select(
        "id",
        "course_id",
        "grade_status",
        "enrolled_at_quarter",
        "course_dept",
        "enrolled_at_label",
        F.coalesce(F.col("grade_status_or_unknown"), F.lit("UNKNOWN")).alias("grade_status_or_unknown_or_unknown"),
        F.abs(F.col("enrolled_at_quarter") - 3).alias("enrolled_at_quarter_distance"),
    )
    .orderBy(F.col("enrolled_at_label").desc_nulls_first(), "course_dept")
)
