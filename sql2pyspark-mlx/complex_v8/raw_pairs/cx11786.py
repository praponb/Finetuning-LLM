from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_is_international_stu = Window.partitionBy("student_is_international", "student_cohort")
w_student_id_student_is_intern = Window.partitionBy("student_id", "student_is_international").orderBy(F.col("course_credits").asc_nulls_last(), "id").rowsBetween(-3, -1)
w_all_by_course_credits_moving = Window.orderBy(F.col("course_credits").desc_nulls_first(), "id").rowsBetween(-1, Window.currentRow)
w_term = Window.partitionBy("term")
w_course_credits_by_enrolled_a = Window.partitionBy("course_credits").orderBy(F.col("enrolled_at").asc_nulls_last(), "id")
w_all_by_course_credits = Window.orderBy(F.col("course_credits").desc_nulls_first(), "id")
w_enrolled_at_dense_rank = Window.partitionBy("enrolled_at_dense_rank")
w_enrolled_at_dense_rank_disti = Window.partitionBy("enrolled_at_dense_rank", "distinct_grade_status_or_unknown_in_student_is_i")
w_distinct_grade_status_or_unk = Window.partitionBy("distinct_grade_status_or_unknown_in_student_is_i", "next_course_credits").orderBy("distinct_grade_status_or_unknown_in_student_is_i", "id").rowsBetween(-6, Window.currentRow)
w_distinct_grade_status_or_unk_2 = Window.partitionBy("distinct_grade_status_or_unknown_in_student_is_i", "next_course_credits").orderBy("distinct_grade_status_or_unknown_in_student_is_i", "id").rowsBetween(-1, Window.currentRow)
w_enrolled_at_dense_rank_by_en = Window.partitionBy("enrolled_at_dense_rank").orderBy("enrolled_at_dense_rank", "id")

valid = (
    df_enrollments
    .filter(~F.col("term").isin("2025T2", "2025T1"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.date_format(F.col("enrolled_at"), "yyyy-MM-dd HH:00").alias("enrolled_at_label"),
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
    )
)

combined_details_student = (
    valid.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        F.col("st.cohort").alias("student_cohort"),
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        F.col("st.is_international").alias("student_is_international"),
        F.col("c.credits").alias("course_credits"),
    )
)

sequenced = (
    combined_details_student
    .select(
        "id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        "enrolled_at_label",
        "course_credits",
        F.size(F.collect_set("grade_status_or_unknown").over(w_student_is_international_stu)).alias("distinct_grade_status_or_unknown_in_student_is_i"),
        F.round(F.avg("course_credits").over(w_student_id_student_is_intern), 2).alias("course_credits_avg_3_prior"),
        F.round(F.avg("score").over(w_all_by_course_credits_moving), 2).alias("score_avg_2"),
        F.round(F.col("course_credits") / F.nullif(F.sum("course_credits").over(w_student_is_international_stu), F.lit(0)), 4).alias("course_credits_share_of_student_is_international"),
        F.size(F.collect_set("student_cohort").over(w_term)).alias("distinct_student_cohort_in_term"),
        F.dense_rank().over(w_course_credits_by_enrolled_a).alias("enrolled_at_dense_rank"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_student_is_international_stu), F.lit(0)), 4).alias("score_share_of_student_is_international_student_"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_term), F.lit(0)), 4).alias("score_share_of_term"),
        F.lead("course_credits").over(w_all_by_course_credits).alias("next_course_credits"),
    )
)

with_windows = (
    sequenced
    .select(
        "*",
        F.size(F.collect_set("course_id").over(w_enrolled_at_dense_rank)).alias("distinct_course_id_in_enrolled_at_dense_rank"),
        F.count(F.lit(1)).over(w_enrolled_at_dense_rank_disti).alias("rows_in_enrolled_at_dense_rank_distinct_grade_st"),
        F.sum("course_credits").over(w_enrolled_at_dense_rank).alias("enrolled_at_dense_rank_sum_course_credits"),
        F.max("next_course_credits").over(w_distinct_grade_status_or_unk).alias("next_course_credits_max_7"),
        F.round(F.avg("score_share_of_term").over(w_distinct_grade_status_or_unk_2), 2).alias("score_share_of_term_avg_2"),
        F.lead("course_credits_avg_3_prior", 3, 0).over(w_enrolled_at_dense_rank_by_en).alias("next3_course_credits_avg_3_prior"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        "course_credits",
        "distinct_grade_status_or_unknown_in_student_is_i",
        "course_credits_avg_3_prior",
        "score_avg_2",
        "course_credits_share_of_student_is_international",
        "distinct_student_cohort_in_term",
        "enrolled_at_dense_rank",
        "score_share_of_student_is_international_student_",
        "score_share_of_term",
        "next_course_credits",
        "rows_in_enrolled_at_dense_rank_distinct_grade_st",
        "enrolled_at_dense_rank_sum_course_credits",
        "next_course_credits_max_7",
        "score_share_of_term_avg_2",
        "next3_course_credits_avg_3_prior",
        F.length(F.trim(F.col("term"))).alias("term_len"),
        F.lower(F.trim(F.col("enrolled_at_label"))).alias("enrolled_at_label_clean"),
    )
    .orderBy(F.col("next3_course_credits_avg_3_prior").desc_nulls_first())
)
