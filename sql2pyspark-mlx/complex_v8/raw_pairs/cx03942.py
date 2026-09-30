from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_level_student_major = Window.partitionBy("course_level", "student_major")
w_all_by_course_credits_prior = Window.orderBy(F.col("course_credits").asc_nulls_last(), "id").rowsBetween(-2, -1)
w_course_level = Window.partitionBy("course_level")
w_student_enrolled_on_month_te = Window.partitionBy(F.date_trunc("month", "student_enrolled_on"), "term").orderBy(F.col("student_enrolled_on").asc_nulls_last(), "id")
w_all_by_score_full = Window.orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_student_major_course_level_b = Window.partitionBy("student_major", "course_level").orderBy(F.col("student_gpa").desc_nulls_first(), "id")
w_all_by_course_credits_full = Window.orderBy("course_credits", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_id = Window.partitionBy("course_id")
w_enrolled_at_to_date_course_i = Window.partitionBy(F.to_date("enrolled_at"), "course_id").orderBy(F.col("course_credits").asc_nulls_last(), "id")
w_all_by_course_credits = Window.orderBy(F.col("course_credits").asc_nulls_last(), "id")
w_all_by_course_credits_2 = Window.orderBy("course_credits", "id")
w_next_student_enrolled_on_mon = Window.partitionBy(F.date_trunc("month", "next_student_enrolled_on"))
w_all_by_student_enrolled_on_m = Window.orderBy(F.col("student_enrolled_on").desc(), "id").rowsBetween(-1, Window.currentRow)
w_student_id = Window.partitionBy("student_id")
w_term_course_id_by_first_cour = Window.partitionBy("term", "course_id").orderBy("first_course_credits")

base = (
    df_enrollments
    .filter(F.col("grade_status") == "INCOMPLETE")
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.round(F.col("score") * 0.9, 2).alias("score_adj"),
    )
)

combined_details = (
    base.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.enrolled_at",
        F.col("st.gpa").alias("student_gpa"),
        F.col("st.major").alias("student_major"),
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        F.col("c.level").alias("course_level"),
        F.col("c.credits").alias("course_credits"),
    )
)

ranked = (
    combined_details
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "student_enrolled_on",
        "course_level",
        "course_credits",
        F.round(F.avg("score").over(w_course_level_student_major), 2).alias("course_level_student_major_avg_score"),
        F.max("score").over(w_all_by_course_credits_prior).alias("score_max_2_prior"),
        F.count(F.lit(1)).over(w_course_level_student_major).alias("rows_in_course_level_student_major"),
        F.max("course_credits").over(w_course_level).alias("course_level_max_course_credits"),
        F.round(F.col("score") - F.avg("score").over(w_course_level), 2).alias("score_vs_course_level_avg"),
        F.lead("student_enrolled_on").over(w_student_enrolled_on_month_te).alias("next_student_enrolled_on"),
        F.last("score").over(w_all_by_score_full).alias("last_score"),
        F.ntile(5).over(w_student_major_course_level_b).alias("student_gpa_quintile"),
        F.first("course_credits").over(w_all_by_course_credits_full).alias("first_course_credits"),
        F.rank().over(w_student_enrolled_on_month_te).alias("student_enrolled_on_rank"),
        F.size(F.collect_set("student_id").over(w_course_id)).alias("distinct_student_id_in_course_id"),
        F.lead("student_gpa", 1, 0).over(w_enrolled_at_to_date_course_i).alias("next_student_gpa"),
        F.round(F.cume_dist().over(w_all_by_course_credits), 3).alias("course_credits_cume_dist"),
    )
)

leaders = (
    ranked
    .filter((F.col("student_enrolled_on_rank") <= 2) & (F.col("score_max_2_prior") < 195.0))
    .drop("student_enrolled_on_rank")
)

windowed = (
    leaders
    .select(
        "id",
        "student_id",
        "term",
        "student_enrolled_on",
        "course_level",
        "course_credits",
        "course_level_student_major_avg_score",
        "score_max_2_prior",
        "course_level_max_course_credits",
        "score_vs_course_level_avg",
        "next_student_enrolled_on",
        "last_score",
        "student_gpa_quintile",
        "distinct_student_id_in_course_id",
        "next_student_gpa",
        "course_credits_cume_dist",
        F.dense_rank().over(w_all_by_course_credits_2).alias("course_credits_dense_rank"),
        F.round(F.col("course_credits_cume_dist") / F.nullif(F.sum("course_credits_cume_dist").over(w_next_student_enrolled_on_mon), F.lit(0)), 4).alias("course_credits_cume_dist_share_of_next_student_e"),
        F.sum("first_course_credits").over(w_all_by_student_enrolled_on_m).alias("first_course_credits_sum_2"),
        F.round(F.col("score_vs_course_level_avg") - F.avg("score_vs_course_level_avg").over(w_student_id), 2).alias("score_vs_course_level_avg_vs_student_id_avg"),
        F.round(F.percent_rank().over(w_term_course_id_by_first_cour), 3).alias("first_course_credits_percent_rank"),
        F.round(F.cume_dist().over(w_all_by_course_credits_2), 3).alias("course_credits_cume_dist_2"),
    )
)

grouped = (
    windowed
    .filter(F.col("course_credits_cume_dist_2") <= 0.9)
    .groupBy("first_course_credits_sum_2")
    .agg(
        F.round(F.sum("course_credits") / F.nullif(F.sum("first_course_credits_percent_rank"), F.lit(0)), 3).alias("course_credits_to_first_course_credits_percent_r"),
        F.countDistinct("student_id").alias("distinct_student_ids"),
        F.min("course_level_student_major_avg_score").alias("min_course_level_student_major_avg_score"),
        F.count(F.when(F.col("term") == "2024T1", 1)).alias("n_term_2024t1"),
        F.round(100 * F.count(F.when(F.col("term") != "2025T1", 1)) / F.count("*"), 1).alias("pct_term_2025t1"),
    )
)

result = (
    grouped
    .select(
        "first_course_credits_sum_2",
        "course_credits_to_first_course_credits_percent_r",
        "distinct_student_ids",
        "min_course_level_student_major_avg_score",
        F.abs(F.col("distinct_student_ids") - 6).alias("distinct_student_ids_distance"),
        F.least(F.col("n_term_2024t1"), F.col("distinct_student_ids")).alias("least_n_term_2024t1_distinct_student_ids"),
    )
)
