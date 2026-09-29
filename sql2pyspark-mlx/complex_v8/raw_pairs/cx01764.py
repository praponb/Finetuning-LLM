from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_score_tier_student_id_by_cou = Window.partitionBy("score_tier", "student_id").orderBy(F.col("course_credits").asc_nulls_last(), "id")
w_course_credits = Window.partitionBy("course_credits")
w_course_id_student_is_interna = Window.partitionBy("course_id", "student_is_international")
w_student_major_by_score_full = Window.partitionBy("student_major").orderBy(F.col("score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_student_is_international_cou = Window.partitionBy("student_is_international", "course_credits").orderBy(F.col("score").desc(), "id")
w_student_is_international_cou_2 = Window.partitionBy("student_is_international", "course_credits").orderBy(F.col("score").desc())
w_score_tier_student_id_by_cou_2 = Window.partitionBy("score_tier", "student_id").orderBy(F.col("course_credits").asc_nulls_last())
w_student_is_international_cou_3 = Window.partitionBy("student_is_international", "course_credits").orderBy(F.col("score").desc(), "id").rowsBetween(-3, -1)
w_distinct_term_in_course_cred = Window.partitionBy("distinct_term_in_course_credits", "grade_status_group")

cleaned_enrollments = (
    df_enrollments
    .filter(F.col("course_id") >= 11)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.when(F.col("score") >= 73.7, "HIGH").when(F.col("score") >= 17.4, "MEDIUM").otherwise("LOW").alias("score_tier"),
        F.when(F.col("grade_status").isin("FAIL", "PASS"), "group_a").when(F.col("grade_status") == "INCOMPLETE", "group_b").otherwise("other").alias("grade_status_group"),
    )
)

with_parent = (
    cleaned_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.score_tier",
        "s.grade_status_group",
        F.col("st.is_international").alias("student_is_international"),
        F.col("st.gpa").alias("student_gpa"),
        F.col("st.major").alias("student_major"),
        F.col("c.credits").alias("course_credits"),
    )
)

ranked = (
    with_parent
    .select(
        "*",
        F.round(F.percent_rank().over(w_score_tier_student_id_by_cou), 3).alias("course_credits_percent_rank"),
        F.count(F.lit(1)).over(w_course_credits).alias("rows_in_course_credits"),
        F.size(F.collect_set("grade_status_group").over(w_course_id_student_is_interna)).alias("distinct_grade_status_group_in_course_id_student"),
        F.first("course_credits").over(w_student_major_by_score_full).alias("first_course_credits"),
        F.row_number().over(w_student_is_international_cou).alias("score_seq"),
        F.rank().over(w_student_is_international_cou_2).alias("score_rank"),
        F.ntile(3).over(w_student_is_international_cou).alias("score_tercile"),
        F.lag("student_gpa").over(w_student_is_international_cou).alias("prev_student_gpa"),
        F.size(F.collect_set("score_tier").over(w_course_id_student_is_interna)).alias("distinct_score_tier_in_course_id_student_is_inte"),
        F.round(F.col("score") - F.avg("score").over(w_course_id_student_is_interna), 2).alias("score_vs_course_id_student_is_international_avg"),
        F.size(F.collect_set("term").over(w_course_credits)).alias("distinct_term_in_course_credits"),
        F.sum("student_gpa").over(w_course_credits).alias("course_credits_sum_student_gpa"),
        F.dense_rank().over(w_score_tier_student_id_by_cou_2).alias("course_credits_dense_rank"),
        F.sum("student_gpa").over(w_student_is_international_cou_3).alias("student_gpa_sum_3_prior"),
        F.round(F.col("course_credits") / F.nullif(F.sum("course_credits").over(w_course_credits), F.lit(0)), 4).alias("course_credits_share_of_course_credits"),
    )
)

windowed = (
    ranked
    .withColumn("distinct_student_major_in_distinct_term_in_cours", F.size(F.collect_set("student_major").over(w_distinct_term_in_course_cred)))
    .withColumn("rows_in_distinct_term_in_course_credits_grade_st", F.count(F.lit(1)).over(w_distinct_term_in_course_cred))
)

result = (
    windowed
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "score_tier",
        "grade_status_group",
        "student_is_international",
        "student_gpa",
        "student_major",
        "course_credits_percent_rank",
        "rows_in_course_credits",
        "score_seq",
        "score_rank",
        "score_tercile",
        "distinct_score_tier_in_course_id_student_is_inte",
        "score_vs_course_id_student_is_international_avg",
        "distinct_term_in_course_credits",
        "course_credits_sum_student_gpa",
        "student_gpa_sum_3_prior",
        "distinct_student_major_in_distinct_term_in_cours",
        "rows_in_distinct_term_in_course_credits_grade_st",
        F.when(F.col("term").isin("2025T2", "2024T2"), "group_a").when(F.col("term") == "2025T1", "group_b").otherwise("other").alias("term_group"),
    )
    .orderBy(F.col("score_rank").desc_nulls_first())
)
