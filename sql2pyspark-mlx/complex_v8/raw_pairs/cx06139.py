from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("term", "student_id").orderBy(F.col("course_credits").desc(), F.col("id").desc())

scoped = (
    df_enrollments
    .filter(F.col("enrolled_at") >= F.lit("2025-01-25 11:00:00").cast("timestamp"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.substring(F.trim(F.col("term")), 1, 2).alias("term_prefix"),
    )
)

with_details_course = (
    scoped.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .filter(F.col("c.level").isin("intro", "core"))
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.enrolled_at",
        F.col("c.level").alias("course_level"),
        "st.student_name",
    )
)

combined_details = (
    with_details_course.alias("s")
    .join(df_courses.alias("c"), (F.col("c.id") == F.col("s.course_id")) & (F.col("c.credits") < 4), "inner")
    .filter(F.col("c.credits").between(2, 3))
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.enrolled_at",
        "s.course_level",
        "s.student_name",
        F.col("c.credits").alias("course_credits"),
        F.col("c.dept").alias("course_dept"),
    )
)

filtered = (
    combined_details.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.score") <= 74.9) & (F.col("e.grade_status").isin("FAIL", "WITHDRAWN", "INCOMPLETE")), "left_semi")
    .filter(F.col("course_credits").between(1, 3))
)

latest_per_key = (
    filtered
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

joined_course = (
    latest_per_key.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status",
        "s.enrolled_at",
        "s.course_level",
        "s.student_name",
        F.col("c.credits").alias("course_credits_2"),
        F.col("c.dept").alias("course_dept_2"),
        F.col("c.level").alias("course_level_2"),
    )
)

result = (
    joined_course
    .select(
        "id",
        "student_id",
        "course_id",
        "grade_status",
        "course_level",
        "course_credits_2",
        "course_dept_2",
        F.date_trunc("quarter", F.col("enrolled_at")).alias("enrolled_at_quarter"),
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
    )
    .orderBy(F.col("student_id").desc_nulls_first())
)
