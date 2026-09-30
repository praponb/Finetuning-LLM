from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("student_major").orderBy(F.col("student_enrolled_on").desc(), F.col("id").desc())

valid_enrollments = (
    df_enrollments
    .filter((F.col("grade_status").isNotNull()) | (F.col("course_id") >= 5))
)

with_parent = (
    valid_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status",
        "s.enrolled_at",
        F.col("st.major").alias("student_major"),
        F.col("c.level").alias("course_level"),
        F.col("c.credits").alias("course_credits"),
    )
)

scored = (
    with_parent
    .filter(F.col("student_id") >= 18)
    .select(
        "*",
        F.year(F.col("enrolled_at")).alias("enrolled_at_year"),
        F.concat_ws("-", F.col("grade_status"), F.col("student_major")).alias("grade_status_student_major_label"),
        F.coalesce(F.col("course_credits"), F.lit(0)).alias("course_credits_filled"),
    )
)

enriched_student = (
    scored.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status",
        "s.enrolled_at",
        "s.student_major",
        "s.course_level",
        "s.course_credits",
        "s.enrolled_at_year",
        "s.grade_status_student_major_label",
        "s.course_credits_filled",
        F.col("st.enrolled_on").alias("student_enrolled_on"),
    )
)

deduped = (
    enriched_student
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

enriched = (
    deduped.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .filter(~F.col("st.is_international"))
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.enrolled_at",
        "s.student_major",
        "s.course_level",
        "s.course_credits",
        "s.enrolled_at_year",
        "s.grade_status_student_major_label",
        "s.course_credits_filled",
        "s.student_enrolled_on",
        "st.student_name",
        F.col("st.is_international").alias("student_is_international"),
        F.col("c.level").alias("course_level_2"),
    )
)

derived = (
    enriched
    .select(
        "*",
        F.substring(F.trim(F.col("course_level_2")), 1, 2).alias("course_level_2_prefix"),
        (F.col("enrolled_at_year") - F.col("course_credits")).alias("enrolled_at_year_minus_course_credits"),
    )
)

result = (
    derived
    .select(
        "id",
        "course_id",
        "enrolled_at",
        "student_major",
        "course_level",
        "course_credits",
        "course_credits_filled",
        "student_name",
        "course_level_2",
        "course_level_2_prefix",
        "enrolled_at_year_minus_course_credits",
    )
    .orderBy(F.col("course_level_2").desc())
)
