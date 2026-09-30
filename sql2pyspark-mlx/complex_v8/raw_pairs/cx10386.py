from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id_course_level = Window.partitionBy("course_id", "course_level")
w_enrollment_grade_status_coun = Window.partitionBy("enrollment_grade_status_count").orderBy(F.col("enrolled_at").desc())

recent_enrollments = (
    df_enrollments
    .filter(F.col("term") == "2025T2")
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
    )
)

combined_details_course = (
    recent_enrollments.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .filter(F.col("c.dept").isin("Art", "Biz", "CS"))
    .select(
        "s.*",
        F.col("c.level").alias("course_level"),
        F.col("c.credits").alias("course_credits"),
        F.col("c.dept").alias("course_dept"),
        "st.student_name",
        F.col("st.major").alias("student_major"),
    )
)

with_activity = (
    combined_details_course.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.score") > 38.1) & (F.col("e.student_id") <= 2), "left_semi")
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.count(F.when(F.col("grade_status") != "WITHDRAWN", 1)).alias("enrollment_grade_status_count"),
    )
)

with_stats = (
    with_activity.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_grade_status_count"), F.lit(0)).alias("enrollment_grade_status_count"),
    )
)

ranked = (
    with_stats
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        "course_level",
        "student_name",
        "student_major",
        "enrollment_grade_status_count",
        F.size(F.collect_set("course_dept").over(w_course_id_course_level)).alias("distinct_course_dept_in_course_id_course_level"),
        F.round(F.percent_rank().over(w_enrollment_grade_status_coun), 3).alias("enrolled_at_percent_rank"),
        F.round(F.col("course_credits") / F.nullif(F.sum("course_credits").over(w_course_id_course_level), F.lit(0)), 4).alias("course_credits_share_of_course_id_course_level"),
    )
)

with_details = (
    ranked.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.enrolled_at",
        "s.course_level",
        "s.student_name",
        "s.student_major",
        "s.enrollment_grade_status_count",
        "s.enrolled_at_percent_rank",
        "s.course_credits_share_of_course_id_course_level",
        F.col("c.dept").alias("course_dept"),
        "c.course_code",
        F.col("c.level").alias("course_level_2"),
        F.col("st.is_international").alias("student_is_international"),
        F.col("st.major").alias("student_major_2"),
    )
)

qualified = (
    with_details.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left_semi")
)

result = (
    qualified
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        "course_level",
        "student_name",
        "student_major",
        "enrollment_grade_status_count",
        "enrolled_at_percent_rank",
        "course_credits_share_of_course_id_course_level",
        "course_dept",
        "course_level_2",
        "student_major_2",
        F.hour(F.col("enrolled_at")).alias("enrolled_at_hour"),
    )
    .orderBy(F.col("student_name").desc_nulls_first())
)
