from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_enrolled_at_year_course_id_b = Window.partitionBy(F.year("enrolled_at"), "course_id").orderBy("student_gpa", "id").rowsBetween(-3, Window.currentRow)
w_course_id_grade_status_by_en = Window.partitionBy("course_id", "grade_status").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_student_gpa_ties = Window.orderBy("student_gpa")
w_course_id_by_student_gpa_tie = Window.partitionBy("course_id").orderBy(F.col("student_gpa").desc_nulls_first())
w_term = Window.partitionBy("term")
w_course_id_term = Window.partitionBy("course_id", "term")
w_course_id = Window.partitionBy("course_id")
w_student_gpa_dense_rank_stude = Window.partitionBy("student_gpa_dense_rank", "student_id").orderBy(F.col("student_gpa_max_4").desc(), "id")
w_student_gpa_dense_rank_stude_2 = Window.partitionBy("student_gpa_dense_rank", "student_id").orderBy(F.col("student_gpa_max_4").desc(), "id").rowsBetween(-1, Window.currentRow)
w_grade_status_student_major_b = Window.partitionBy("grade_status", "student_major").orderBy("term_avg_student_gpa")

filtered = (
    df_enrollments
    .filter((F.col("course_id").isNotNull()) & (F.col("term").isin("2025T1", "2024T1")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.lower(F.regexp_replace(F.trim(F.col("term")), " ", "_")).alias("term_slug"),
        F.concat_ws("-", F.col("term"), F.col("grade_status")).alias("term_grade_status_label"),
    )
)

enriched = (
    filtered.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.enrolled_at",
        "s.term_slug",
        "s.term_grade_status_label",
        F.col("st.gpa").alias("student_gpa"),
        F.col("st.major").alias("student_major"),
    )
)

with_trends = (
    enriched
    .select(
        "*",
        F.max("student_gpa").over(w_enrolled_at_year_course_id_b).alias("student_gpa_max_4"),
        F.first("enrolled_at").over(w_course_id_grade_status_by_en).alias("first_enrolled_at"),
        F.nth_value("student_gpa", 2).over(w_course_id_grade_status_by_en).alias("second_student_gpa"),
        F.round(F.cume_dist().over(w_all_by_student_gpa_ties), 3).alias("student_gpa_cume_dist"),
        F.dense_rank().over(w_course_id_by_student_gpa_tie).alias("student_gpa_dense_rank"),
        F.round(F.avg("student_gpa").over(w_term), 2).alias("term_avg_student_gpa"),
        F.round(F.col("student_gpa") - F.avg("student_gpa").over(w_course_id_term), 2).alias("student_gpa_vs_course_id_term_avg"),
        F.round(F.col("student_gpa") - F.avg("student_gpa").over(w_term), 2).alias("student_gpa_vs_term_avg"),
        F.round(F.col("student_gpa") - F.avg("student_gpa").over(w_course_id), 2).alias("student_gpa_vs_course_id_avg"),
    )
)

ranked = (
    with_trends
    .select(
        "id",
        "student_id",
        "course_id",
        "grade_status",
        "enrolled_at",
        "student_gpa",
        "student_major",
        "first_enrolled_at",
        "second_student_gpa",
        "student_gpa_dense_rank",
        "student_gpa_vs_term_avg",
        "student_gpa_vs_course_id_avg",
        F.round(F.cume_dist().over(w_student_gpa_dense_rank_stude), 3).alias("student_gpa_max_4_cume_dist"),
        F.lead("student_gpa_vs_term_avg").over(w_student_gpa_dense_rank_stude).alias("next_student_gpa_vs_term_avg"),
        F.round(F.avg("second_student_gpa").over(w_course_id), 2).alias("course_id_avg_second_student_gpa"),
        F.max("student_gpa_vs_course_id_avg").over(w_student_gpa_dense_rank_stude_2).alias("student_gpa_vs_course_id_avg_max_2"),
        F.dense_rank().over(w_grade_status_student_major_b).alias("term_avg_student_gpa_dense_rank"),
    )
)

result = (
    ranked
    .filter(F.col("student_gpa_max_4_cume_dist") < 0.6)
    .select(
        "id",
        "student_id",
        "grade_status",
        "enrolled_at",
        "student_gpa",
        "student_major",
        "first_enrolled_at",
        "second_student_gpa",
        "student_gpa_vs_term_avg",
        "student_gpa_vs_course_id_avg",
        "student_gpa_max_4_cume_dist",
        "next_student_gpa_vs_term_avg",
        "term_avg_student_gpa_dense_rank",
        F.substring(F.trim(F.col("student_major")), 1, 3).alias("student_major_prefix"),
    )
    .orderBy("next_student_gpa_vs_term_avg", "id")
    .limit(20)
)
