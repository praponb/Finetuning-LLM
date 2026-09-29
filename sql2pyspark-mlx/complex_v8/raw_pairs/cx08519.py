from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term_or_unknown_student_id_b = Window.partitionBy("term_or_unknown", "student_id").orderBy(F.col("score").desc(), "id").rowsBetween(-3, -1)
w_term_or_unknown_student_id_b_2 = Window.partitionBy("term_or_unknown", "student_id").orderBy(F.col("score").desc(), "id")
w_student_id = Window.partitionBy("student_id")

filtered_enrollments = (
    df_enrollments
    .filter(F.col("student_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.coalesce(F.col("term"), F.lit("UNKNOWN")).alias("term_or_unknown"),
        F.when(F.col("score") >= 57.3, "large").when(F.col("score") >= 20.2, "medium").otherwise("small").alias("score_tier"),
        F.concat_ws("-", F.col("term"), F.col("grade_status")).alias("term_grade_status_label"),
    )
)

ranked = (
    filtered_enrollments
    .select(
        "*",
        F.round(F.avg("score").over(w_term_or_unknown_student_id_b), 2).alias("score_avg_3_prior"),
        F.lead("enrolled_at").over(w_term_or_unknown_student_id_b_2).alias("next_enrolled_at"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_student_id), F.lit(0)), 4).alias("score_share_of_student_id"),
        F.lag("enrolled_at", 3).over(w_term_or_unknown_student_id_b_2).alias("prev3_enrolled_at"),
        F.ntile(4).over(w_term_or_unknown_student_id_b_2).alias("score_quartile"),
    )
)

with_parent_course = (
    ranked.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select("s.*", F.col("c.credits").alias("course_credits"))
)

with_parent = (
    with_parent_course.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.enrolled_at",
        "s.term_or_unknown",
        "s.score_tier",
        "s.term_grade_status_label",
        "s.score_avg_3_prior",
        "s.next_enrolled_at",
        "s.score_share_of_student_id",
        "s.prev3_enrolled_at",
        "s.score_quartile",
        "st.student_name",
        F.col("st.gpa").alias("student_gpa"),
        F.col("st.major").alias("student_major"),
    )
)

with_details_course = (
    with_parent.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.term_or_unknown",
        "s.score_tier",
        "s.term_grade_status_label",
        "s.score_avg_3_prior",
        "s.score_share_of_student_id",
        "s.prev3_enrolled_at",
        "s.score_quartile",
        "s.student_name",
        "s.student_gpa",
        "s.student_major",
        F.col("c.credits").alias("course_credits"),
        "c.course_code",
    )
)

with_parent_course_2 = (
    with_details_course.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.term_or_unknown",
        "s.score_tier",
        "s.term_grade_status_label",
        "s.score_share_of_student_id",
        "s.prev3_enrolled_at",
        "s.score_quartile",
        "s.student_gpa",
        "s.student_major",
        "s.course_credits",
        "s.course_code",
        F.col("c.dept").alias("course_dept"),
        F.col("st.gpa").alias("student_gpa_2"),
        F.col("st.student_name").alias("student_student_name"),
    )
)

result = (
    with_parent_course_2
    .cube("student_major", "course_dept", "course_credits")
    .agg(
        F.sum(F.when(F.upper(F.trim(F.col("course_code"))).like("A%"), F.col("student_gpa_2")).otherwise(0)).alias("student_gpa_2_upper_trim_course_code_like"),
        F.countDistinct("student_id").alias("distinct_student_ids"),
        F.countDistinct("term_or_unknown").alias("distinct_term_or_unknowns"),
    )
    .orderBy(
        F.col("student_major").asc_nulls_last(),
        F.col("course_dept").asc_nulls_last(),
        F.col("course_credits").asc_nulls_last(),
    )
)
