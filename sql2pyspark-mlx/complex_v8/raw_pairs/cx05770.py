from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_student_id = Window.partitionBy("grade_status", "student_id")
w_student_cohort_by_enrolled_a = Window.partitionBy("student_cohort").orderBy(F.col("enrolled_at").asc_nulls_last())
w_student_cohort_by_enrolled_a_2 = Window.partitionBy("student_cohort").orderBy(F.col("enrolled_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_student_cohort_by_enrolled_a_3 = Window.partitionBy("student_cohort").orderBy(F.col("enrolled_at").asc_nulls_last(), "id")
w_grade_status_term = Window.partitionBy("grade_status", "term")
w_student_cohort_enrolled_at_t = Window.partitionBy("student_cohort", F.to_date("enrolled_at")).orderBy("score_adj", "id")
w_grade_status_group_student_i = Window.partitionBy("grade_status_group", "student_id").orderBy("score_adj")
w_grade_status_group_student_i_2 = Window.partitionBy("grade_status_group", "student_id").orderBy("score_adj", "id")
w_distinct_grade_status_group = Window.partitionBy("distinct_grade_status_group_in_grade_status_term").orderBy("score_adj_vs_grade_status_student_id_avg", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned_enrollments = (
    df_enrollments
    .filter(F.col("score") <= 62.5)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.round(F.col("score") * 1.07, 2).alias("score_adj"),
        F.when(F.col("grade_status").isin("PASS", "WITHDRAWN"), "group_a").when(F.col("grade_status") == "FAIL", "group_b").otherwise("other").alias("grade_status_group"),
    )
)

joined = (
    cleaned_enrollments.alias("s")
    .join(df_courses.alias("c"), (F.col("c.id") == F.col("s.course_id")) & (F.upper(F.trim(F.col("c.course_code"))).like("C%")), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .filter(F.col("c.level") != "core")
    .select(
        "s.*",
        F.col("c.level").alias("course_level"),
        F.col("st.cohort").alias("student_cohort"),
    )
)

ranked = (
    joined
    .select(
        "*",
        F.size(F.collect_set("course_level").over(w_grade_status_student_id)).alias("distinct_course_level_in_grade_status_student_id"),
        F.round(F.percent_rank().over(w_student_cohort_by_enrolled_a), 3).alias("enrolled_at_percent_rank"),
        F.round(F.col("score") - F.avg("score").over(w_grade_status_student_id), 2).alias("score_vs_grade_status_student_id_avg"),
        F.min("score").over(w_grade_status_student_id).alias("grade_status_student_id_min_score"),
        F.nth_value("enrolled_at", 2).over(w_student_cohort_by_enrolled_a_2).alias("second_enrolled_at"),
        F.lead("enrolled_at", 3).over(w_student_cohort_by_enrolled_a_3).alias("next3_enrolled_at"),
        F.round(F.col("score_adj") - F.avg("score_adj").over(w_grade_status_student_id), 2).alias("score_adj_vs_grade_status_student_id_avg"),
        F.size(F.collect_set("grade_status_group").over(w_grade_status_term)).alias("distinct_grade_status_group_in_grade_status_term"),
        F.lead("enrolled_at", 2).over(w_student_cohort_enrolled_at_t).alias("next2_enrolled_at"),
    )
)

scored = (
    ranked
    .filter(F.col("course_level").isNotNull())
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("term")), " ", "_")).alias("term_slug"),
        F.lower(F.regexp_replace(F.trim(F.col("student_cohort")), " ", "_")).alias("student_cohort_slug"),
    )
)

ranked_2 = (
    scored
    .select(
        "id",
        "student_id",
        "term",
        "score",
        "enrolled_at",
        "score_adj",
        "grade_status_group",
        "course_level",
        "student_cohort",
        "distinct_course_level_in_grade_status_student_id",
        "enrolled_at_percent_rank",
        "score_vs_grade_status_student_id_avg",
        "grade_status_student_id_min_score",
        "score_adj_vs_grade_status_student_id_avg",
        "next2_enrolled_at",
        F.round(F.percent_rank().over(w_grade_status_group_student_i), 3).alias("score_adj_percent_rank"),
        F.row_number().over(w_grade_status_group_student_i_2).alias("seq_no"),
        F.nth_value("enrolled_at_percent_rank", 2).over(w_distinct_grade_status_group).alias("second_enrolled_at_percent_rank"),
    )
)

result = (
    ranked_2
    .select(
        "id",
        "student_id",
        "term",
        "score",
        "grade_status_group",
        "course_level",
        "student_cohort",
        "distinct_course_level_in_grade_status_student_id",
        "score_vs_grade_status_student_id_avg",
        "grade_status_student_id_min_score",
        "score_adj_vs_grade_status_student_id_avg",
        "next2_enrolled_at",
        "score_adj_percent_rank",
        F.least(F.col("score_adj_percent_rank"), F.col("seq_no")).alias("least_score_adj_percent_rank_seq_no"),
        F.lower(F.regexp_replace(F.trim(F.col("grade_status_group")), " ", "_")).alias("grade_status_group_slug"),
    )
    .orderBy(F.col("distinct_course_level_in_grade_status_student_id").desc())
)
