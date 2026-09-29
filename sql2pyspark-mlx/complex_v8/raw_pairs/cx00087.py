from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id_course_dept = Window.partitionBy("course_id", "course_dept")

valid = (
    df_enrollments
    .filter(F.col("grade_status") == "WITHDRAWN")
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.abs(F.col("score") - 56.1).alias("score_distance"),
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
        F.lower(F.regexp_replace(F.trim(F.col("term")), " ", "_")).alias("term_slug"),
    )
)

enriched = (
    valid.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.enrolled_at",
        "s.score_distance",
        "s.term_slug",
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        F.col("st.cohort").alias("student_cohort"),
        F.col("st.gpa").alias("student_gpa"),
        F.col("c.dept").alias("course_dept"),
    )
)

with_activity = (
    enriched
    .crossJoin(enriched.agg(F.min("student_gpa").alias("overall_min_student_gpa")))
    .filter(F.col("student_gpa") > F.col("overall_min_student_gpa"))
    .drop("overall_min_student_gpa")
)

with_windows = (
    with_activity
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "enrolled_at",
        "score_distance",
        "term_slug",
        "student_enrolled_on",
        "student_cohort",
        "student_gpa",
        "course_dept",
        F.round(F.col("score_distance") / F.nullif(F.sum("score_distance").over(w_course_id_course_dept), F.lit(0)), 4).alias("score_distance_share_of_course_id_course_dept"),
        F.max("student_gpa").over(w_course_id_course_dept).alias("course_id_course_dept_max_student_gpa"),
    )
)

result = (
    with_windows
    .groupingSets(
        [["course_id", "course_dept"], ["course_id"], ["course_dept"]],
        "course_id", "course_dept",
    )
    .agg(F.min("student_enrolled_on").alias("first_student_enrolled_on"))
    .select(
        "course_id",
        F.coalesce(F.col("course_dept"), F.lit("ALL")).alias("course_dept"),
        "first_student_enrolled_on",
    )
)
