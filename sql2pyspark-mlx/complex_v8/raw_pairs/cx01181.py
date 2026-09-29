from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id = Window.partitionBy("student_id")

valid_enrollments = (
    df_enrollments
    .filter(F.col("score") >= 5.9)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

combined_details = (
    valid_enrollments.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.enrolled_at",
        F.col("c.level").alias("course_level"),
        F.col("c.dept").alias("course_dept"),
        F.col("c.credits").alias("course_credits"),
        F.col("st.gpa").alias("student_gpa"),
        F.col("st.enrolled_on").alias("student_enrolled_on"),
    )
)

grouped = (
    combined_details
    .groupBy("student_id")
    .agg(
        F.min("score").alias("min_score"),
        F.round(100 * F.count(F.when(F.col("course_dept").isin("Math", "CS"), 1)) / F.count("*"), 1).alias("pct_course_dept_in_math_cs"),
        F.round(F.sum("score") / F.nullif(F.sum("student_gpa"), F.lit(0)), 3).alias("score_to_student_gpa_ratio"),
        F.round(F.stddev("student_gpa"), 2).alias("stddev_student_gpa"),
        F.round(100 * F.count(F.when(~F.col("course_dept").isin("Art", "Bio"), 1)) / F.count("*"), 1).alias("pct_course_dept_not_in_art_bio"),
        F.array_sort(F.collect_set("course_dept")).alias("course_dept_list"),
    )
)

with_trends = (
    grouped
    .select(
        "*",
        F.round(F.col("pct_course_dept_in_math_cs") - F.avg("pct_course_dept_in_math_cs").over(w_student_id), 2).alias("pct_course_dept_in_math_cs_vs_student_id_avg"),
        F.round(F.col("score_to_student_gpa_ratio") / F.nullif(F.sum("score_to_student_gpa_ratio").over(w_student_id), F.lit(0)), 4).alias("score_to_student_gpa_ratio_share_of_student_id"),
        F.max("pct_course_dept_in_math_cs").over(w_student_id).alias("student_id_max_pct_course_dept_in_math_cs"),
        F.round(F.col("score_to_student_gpa_ratio") - F.avg("score_to_student_gpa_ratio").over(w_student_id), 2).alias("score_to_student_gpa_ratio_vs_student_id_avg"),
    )
)

with_children = (
    with_trends.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.student_id",
        "s.min_score",
        "s.score_to_student_gpa_ratio",
        "s.pct_course_dept_in_math_cs_vs_student_id_avg",
        "s.student_id_max_pct_course_dept_in_math_cs",
        "s.score_to_student_gpa_ratio_vs_student_id_avg",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
    )
)

with_parent_enrollment = (
    with_children.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.id") == F.col("s.enrollment_id"), "left")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.student_id",
        "s.min_score",
        "s.score_to_student_gpa_ratio",
        "s.pct_course_dept_in_math_cs_vs_student_id_avg",
        "s.student_id_max_pct_course_dept_in_math_cs",
        "s.enrollment_id",
        "s.enrollment_grade_status",
        "s.enrollment_term",
        "s.enrollment_enrolled_at",
        F.col("e.score").alias("enrollment_score"),
        F.col("e.course_id").alias("enrollment_course_id"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at_2"),
        F.col("st.cohort").alias("student_cohort"),
    )
)

result = (
    with_parent_enrollment
    .select(
        "student_id",
        "score_to_student_gpa_ratio",
        "pct_course_dept_in_math_cs_vs_student_id_avg",
        "enrollment_grade_status",
        "enrollment_enrolled_at",
        "enrollment_score",
        "enrollment_course_id",
        "enrollment_enrolled_at_2",
        "student_cohort",
        F.hour(F.col("enrollment_enrolled_at")).alias("enrollment_enrolled_at_hour"),
    )
    .orderBy(F.col("enrollment_score").asc_nulls_last(), F.col("student_id").desc())
)
