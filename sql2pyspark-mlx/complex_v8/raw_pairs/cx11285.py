from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_credits_by_score = Window.partitionBy("course_credits").orderBy("score", "id")
w_student_cohort_student_id_by = Window.partitionBy("student_cohort", "student_id").orderBy(F.col("score").desc_nulls_first(), "id")
w_all_by_student_gpa_running = Window.orderBy(F.col("student_gpa").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_student_id = Window.partitionBy("student_id")
w_grade_status_student_id = Window.partitionBy("grade_status", "student_id")
w_all_by_student_gpa_ties = Window.orderBy(F.col("student_gpa").desc())
w_term_grade_status_by_course = Window.partitionBy("term", "grade_status").orderBy("course_credits", "id")
w_student_gpa_rank_term_by_sco = Window.partitionBy("student_gpa_rank", "term").orderBy("score_vs_grade_status_student_id_avg", "id").rowsBetween(-2, -1)
w_student_gpa_rank_term_by_sco_2 = Window.partitionBy("student_gpa_rank", "term").orderBy("score_vs_grade_status_student_id_avg", "id").rowsBetween(-6, -1)
w_grade_status_enrolled_at_yea = Window.partitionBy("grade_status", "enrolled_at_year").orderBy("student_gpa", "id")
w_student_gpa_rank_term_by_sco_3 = Window.partitionBy("student_gpa_rank", "term").orderBy("score_vs_grade_status_student_id_avg")
w_grade_status_enrolled_at_yea_2 = Window.partitionBy("grade_status", "enrolled_at_year").orderBy("student_gpa", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid = (
    df_enrollments
    .filter(F.col("grade_status") == "PASS")
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.when(F.col("score") >= 78.8, "HIGH").when(F.col("score") >= 13.9, "MEDIUM").otherwise("LOW").alias("score_tier"),
    )
)

enriched_course = (
    valid.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .filter(F.col("c.credits") >= 2)
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        "s.score_tier",
        "c.course_code",
        F.col("c.level").alias("course_level"),
        F.col("c.credits").alias("course_credits"),
        F.col("st.cohort").alias("student_cohort"),
        F.col("st.gpa").alias("student_gpa"),
    )
)

sequenced = (
    enriched_course
    .select(
        "*",
        F.row_number().over(w_course_credits_by_score).alias("rn"),
        (F.col("course_credits") - F.lag("course_credits").over(w_student_cohort_student_id_by)).alias("course_credits_change"),
        F.count(F.lit(1)).over(w_all_by_student_gpa_running).alias("running_count"),
        F.round(F.avg("student_gpa").over(w_student_id), 2).alias("student_id_avg_student_gpa"),
        F.round(F.col("student_gpa") - F.avg("student_gpa").over(w_grade_status_student_id), 2).alias("student_gpa_vs_grade_status_student_id_avg"),
        F.round(F.col("score") - F.avg("score").over(w_grade_status_student_id), 2).alias("score_vs_grade_status_student_id_avg"),
        F.rank().over(w_all_by_student_gpa_ties).alias("student_gpa_rank"),
        (F.col("course_credits") - F.lag("course_credits").over(w_term_grade_status_by_course)).alias("course_credits_change_2"),
        F.lead("score").over(w_course_credits_by_score).alias("next_score"),
        F.round(F.col("student_gpa") - F.avg("student_gpa").over(w_student_id), 2).alias("student_gpa_vs_student_id_avg"),
    )
)

tagged = (
    sequenced
    .select(
        "*",
        F.year(F.col("enrolled_at")).alias("enrolled_at_year"),
        F.greatest(F.col("score"), F.col("student_gpa")).alias("greatest_score_student_gpa"),
        F.date_trunc("week", F.col("enrolled_at")).alias("enrolled_at_week"),
    )
)

with_trends = (
    tagged
    .select(
        "*",
        F.max("student_id_avg_student_gpa").over(w_student_gpa_rank_term_by_sco).alias("student_id_avg_student_gpa_max_2_prior"),
        F.sum("student_id_avg_student_gpa").over(w_student_gpa_rank_term_by_sco_2).alias("student_id_avg_student_gpa_sum_6_prior"),
        F.row_number().over(w_grade_status_enrolled_at_yea).alias("seq_no"),
        F.dense_rank().over(w_student_gpa_rank_term_by_sco_3).alias("score_vs_grade_status_student_id_avg_dense_rank"),
        F.round(F.avg("student_gpa_rank").over(w_student_gpa_rank_term_by_sco_2), 2).alias("student_gpa_rank_avg_6_prior"),
        F.nth_value("student_gpa", 2).over(w_grade_status_enrolled_at_yea_2).alias("second_student_gpa"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "student_id",
        "term",
        "score_tier",
        "course_code",
        "course_level",
        "student_cohort",
        "rn",
        "course_credits_change",
        "student_id_avg_student_gpa",
        "student_gpa_vs_grade_status_student_id_avg",
        "score_vs_grade_status_student_id_avg",
        "student_gpa_rank",
        "course_credits_change_2",
        "next_score",
        "student_gpa_vs_student_id_avg",
        "greatest_score_student_gpa",
        "enrolled_at_week",
        "student_id_avg_student_gpa_max_2_prior",
        "student_id_avg_student_gpa_sum_6_prior",
        "seq_no",
        "score_vs_grade_status_student_id_avg_dense_rank",
        "student_gpa_rank_avg_6_prior",
        "second_student_gpa",
        F.round(F.col("student_gpa") / F.nullif(F.col("enrolled_at_year"), F.lit(0)), 3).alias("student_gpa_per_enrolled_at_year"),
    )
)
