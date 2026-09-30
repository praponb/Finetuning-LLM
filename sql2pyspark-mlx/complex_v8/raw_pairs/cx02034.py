from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id = Window.partitionBy("course_id")
w_course_level = Window.partitionBy("course_level")
w_all_by_score_distance = Window.orderBy("score_distance", "id")
w_term_by_score_distance_ties = Window.partitionBy("term").orderBy(F.col("score_distance").desc())
w_course_credits = Window.partitionBy("course_credits")
w_all_by_score_distance_ties = Window.orderBy("score_distance")
w_all_by_score_distance_prior = Window.orderBy("score_distance", "id").rowsBetween(-3, -1)
w_student_id_term_by_score_dis = Window.partitionBy("student_id", "term").orderBy("score_distance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_term = Window.partitionBy("term")
w_term_by_distinct_course_leve = Window.partitionBy("term").orderBy(F.col("distinct_course_level_in_course_credits").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_term_by_distinct_course_leve_2 = Window.partitionBy("term").orderBy(F.col("distinct_course_level_in_course_credits").desc(), "id")
w_term_by_distinct_course_leve_3 = Window.partitionBy("term").orderBy(F.col("distinct_course_level_in_course_credits").desc())

scoped_enrollments = (
    df_enrollments
    .filter(F.col("course_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.abs(F.col("score") - 62.5).alias("score_distance"),
        F.date_format(F.col("enrolled_at"), "yyyy-MM-dd HH:00").alias("enrolled_at_label"),
    )
)

combined_details_course = (
    scoped_enrollments.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.score_distance",
        "s.enrolled_at_label",
        "c.course_code",
        F.col("c.credits").alias("course_credits"),
        F.col("c.level").alias("course_level"),
    )
)

metrics = (
    combined_details_course
    .select(
        "id",
        "course_id",
        "term",
        "grade_status",
        "score_distance",
        "enrolled_at_label",
        "course_code",
        "course_level",
        F.size(F.collect_set("student_id").over(w_course_id)).alias("distinct_student_id_in_course_id"),
        F.count(F.lit(1)).over(w_course_level).alias("rows_in_course_level"),
        F.lead("score_distance", 1, 0).over(w_all_by_score_distance).alias("next_score_distance"),
        F.round(F.percent_rank().over(w_term_by_score_distance_ties), 3).alias("score_distance_percent_rank"),
        F.count(F.lit(1)).over(w_course_credits).alias("rows_in_course_credits"),
        F.round(F.col("score_distance") - F.avg("score_distance").over(w_course_id), 2).alias("score_distance_vs_course_id_avg"),
        F.dense_rank().over(w_all_by_score_distance_ties).alias("score_distance_dense_rank"),
        F.round(F.avg("course_credits").over(w_all_by_score_distance_prior), 2).alias("course_credits_avg_3_prior"),
        F.size(F.collect_set("course_level").over(w_course_credits)).alias("distinct_course_level_in_course_credits"),
        F.max("course_credits").over(w_student_id_term_by_score_dis).alias("max_course_credits_so_far"),
    )
)

windowed = (
    metrics
    .select(
        "id",
        "course_id",
        "term",
        "grade_status",
        "score_distance",
        "course_code",
        "score_distance_percent_rank",
        "score_distance_vs_course_id_avg",
        "score_distance_dense_rank",
        "course_credits_avg_3_prior",
        "max_course_credits_so_far",
        F.round(F.col("course_credits_avg_3_prior") / F.nullif(F.sum("course_credits_avg_3_prior").over(w_term), F.lit(0)), 4).alias("course_credits_avg_3_prior_share_of_term"),
        F.nth_value("next_score_distance", 2).over(w_term_by_distinct_course_leve).alias("second_next_score_distance"),
        F.ntile(3).over(w_term_by_distinct_course_leve_2).alias("distinct_course_level_in_course_credits_tercile"),
        F.round(F.cume_dist().over(w_term_by_distinct_course_leve_3), 3).alias("distinct_course_level_in_course_credits_cume_dis"),
        F.size(F.collect_set("course_id").over(w_term)).alias("distinct_course_id_in_term"),
        F.lead("score_distance_dense_rank", 3, 0).over(w_term_by_distinct_course_leve_2).alias("next3_score_distance_dense_rank"),
    )
)

result = (
    windowed
    .select(
        "id",
        "course_id",
        "grade_status",
        "score_distance",
        "score_distance_percent_rank",
        "score_distance_vs_course_id_avg",
        "score_distance_dense_rank",
        "course_credits_avg_3_prior",
        "max_course_credits_so_far",
        "course_credits_avg_3_prior_share_of_term",
        "second_next_score_distance",
        "distinct_course_level_in_course_credits_tercile",
        "distinct_course_level_in_course_credits_cume_dis",
        "distinct_course_id_in_term",
        "next3_score_distance_dense_rank",
    )
    .orderBy("course_credits_avg_3_prior_share_of_term")
)
