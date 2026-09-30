from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_student_id_by_s = Window.partitionBy("grade_status", "student_id").orderBy(F.col("score").desc(), "id").rowsBetween(-1, Window.currentRow)
w_student_id = Window.partitionBy("student_id")
w_course_credits = Window.partitionBy("course_credits")
w_student_id_course_credits_by = Window.partitionBy("student_id", "course_credits").orderBy(F.col("score").desc(), "id").rowsBetween(-6, -1)
w_enrolled_at_month_course_cre = Window.partitionBy(F.date_trunc("month", "enrolled_at"), "course_credits").orderBy(F.col("enrolled_at").asc_nulls_last(), "id")
w_grade_status_student_id_by_s_2 = Window.partitionBy("grade_status", "student_id").orderBy(F.col("score").desc(), "id")
w_student_id_course_credits_by_2 = Window.partitionBy("student_id", "course_credits").orderBy(F.col("score").desc(), "id")
w_course_id_student_id_by_scor = Window.partitionBy("course_id", "student_id").orderBy("score", "id")
w_all_by_score_sum_2_ties = Window.orderBy("score_sum_2")
w_all_by_score_sum_2_running = Window.orderBy("score_sum_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_score_sum_2_full = Window.orderBy("score_sum_2", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_grade_status_distinct_studen = Window.partitionBy("grade_status", "distinct_student_id_in_course_credits")
w_score_seq_student_id_by_scor = Window.partitionBy("score_seq", "student_id").orderBy(F.col("score_rank").asc_nulls_last(), "id")
w_all_by_score_sum_2_prior_3 = Window.orderBy("score_sum_2", "id").rowsBetween(-3, -1)

prepared_enrollments = (
    df_enrollments
    .filter((F.col("course_id").isNotNull()) | (F.col("term").isNotNull()))
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

with_details = (
    prepared_enrollments.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        F.col("c.credits").alias("course_credits"),
    )
)

sequenced = (
    with_details
    .select(
        "*",
        F.sum("score").over(w_grade_status_student_id_by_s).alias("score_sum_2"),
        F.sum("course_credits").over(w_student_id).alias("student_id_sum_course_credits"),
        F.min("course_credits").over(w_course_credits).alias("course_credits_min_course_credits"),
        F.round(F.avg("score").over(w_student_id_course_credits_by), 2).alias("score_avg_6_prior"),
        F.round(F.col("course_credits") - F.avg("course_credits").over(w_student_id), 2).alias("course_credits_vs_student_id_avg"),
        F.round(F.avg("score").over(w_course_credits), 2).alias("course_credits_avg_score"),
        (F.col("course_credits") - F.lag("course_credits").over(w_enrolled_at_month_course_cre)).alias("course_credits_change"),
        F.size(F.collect_set("student_id").over(w_course_credits)).alias("distinct_student_id_in_course_credits"),
        F.round(F.col("score") - F.avg("score").over(w_course_credits), 2).alias("score_vs_course_credits_avg"),
        F.round(F.col("score") - F.avg("score").over(w_student_id), 2).alias("score_vs_student_id_avg"),
        F.rank().over(w_grade_status_student_id_by_s_2).alias("score_rank"),
        F.ntile(10).over(w_enrolled_at_month_course_cre).alias("enrolled_at_decile"),
        F.row_number().over(w_student_id_course_credits_by_2).alias("score_seq"),
        F.ntile(4).over(w_course_id_student_id_by_scor).alias("score_quartile"),
        F.round(F.col("course_credits") - F.avg("course_credits").over(w_course_credits), 2).alias("course_credits_vs_course_credits_avg"),
    )
)

annotated = (
    sequenced
    .filter(F.col("score_vs_course_credits_avg") <= 50.0)
    .select(
        "id",
        "student_id",
        "score",
        "grade_status",
        "enrolled_at",
        "score_sum_2",
        "student_id_sum_course_credits",
        "course_credits_vs_student_id_avg",
        "course_credits_change",
        "distinct_student_id_in_course_credits",
        "score_vs_student_id_avg",
        "score_rank",
        "score_seq",
        "score_quartile",
        "course_credits_vs_course_credits_avg",
        F.coalesce(F.col("score_quartile"), F.lit(0)).alias("score_quartile_filled"),
        F.initcap(F.trim(F.col("term"))).alias("term_clean"),
        F.lower(F.regexp_replace(F.trim(F.col("grade_status")), " ", "_")).alias("grade_status_slug"),
    )
)

sequenced_2 = (
    annotated
    .select(
        "id",
        "grade_status",
        "enrolled_at",
        "score_sum_2",
        "student_id_sum_course_credits",
        "course_credits_vs_student_id_avg",
        "course_credits_change",
        "distinct_student_id_in_course_credits",
        "score_vs_student_id_avg",
        "course_credits_vs_course_credits_avg",
        "score_quartile_filled",
        "term_clean",
        F.round(F.cume_dist().over(w_all_by_score_sum_2_ties), 3).alias("score_sum_2_cume_dist"),
        F.count(F.lit(1)).over(w_all_by_score_sum_2_running).alias("running_count"),
        F.nth_value("score", 2).over(w_all_by_score_sum_2_full).alias("second_score"),
        F.round(F.col("score_quartile_filled") - F.avg("score_quartile_filled").over(w_grade_status_distinct_studen), 2).alias("score_quartile_filled_vs_grade_status_distinct_s"),
        F.ntile(3).over(w_score_seq_student_id_by_scor).alias("score_rank_tercile"),
        F.round(F.avg("course_credits_change").over(w_all_by_score_sum_2_prior_3), 2).alias("course_credits_change_avg_3_prior"),
    )
)

result = (
    sequenced_2
    .filter(F.col("course_credits_change_avg_3_prior") > -2.0)
    .select(
        "id",
        "grade_status",
        "enrolled_at",
        "student_id_sum_course_credits",
        "course_credits_change",
        "distinct_student_id_in_course_credits",
        "course_credits_vs_course_credits_avg",
        "score_quartile_filled",
        "term_clean",
        "score_sum_2_cume_dist",
        "running_count",
        "score_quartile_filled_vs_grade_status_distinct_s",
        "score_rank_tercile",
        "course_credits_change_avg_3_prior",
        F.date_trunc("week", F.col("enrolled_at")).alias("enrolled_at_week"),
    )
    .orderBy(F.col("score_sum_2_cume_dist").desc(), "id")
    .limit(5)
)
