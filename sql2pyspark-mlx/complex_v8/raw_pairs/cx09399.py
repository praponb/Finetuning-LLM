from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_score = Window.orderBy(F.col("score").desc(), "id")
w_term_course_id = Window.partitionBy("term", "course_id")
w_all_by_score_moving_3 = Window.orderBy(F.col("score").desc(), "id").rowsBetween(-3, Window.currentRow)
w_term_by_enrolled_at_ties = Window.partitionBy("term").orderBy("enrolled_at")
w_term_by_enrolled_at = Window.partitionBy("term").orderBy("enrolled_at", "id")
w_all_by_score_full = Window.orderBy(F.col("score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_score_prior_6 = Window.orderBy(F.col("score").desc(), "id").rowsBetween(-6, -1)
w_course_id_by_first_enrolled = Window.partitionBy("course_id").orderBy(F.col("first_enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_enrolled_at_quartile_student = Window.partitionBy("enrolled_at_quartile", "student_id")
w_distinct_student_id_in_term = Window.partitionBy("distinct_student_id_in_term_course_id").orderBy("rows_in_term_course_id")
w_next3_enrolled_at_month_enro = Window.partitionBy(F.date_trunc("month", "next3_enrolled_at"), "enrolled_at_quartile").orderBy("enrolled_at", "id").rowsBetween(-2, Window.currentRow)

prepared_enrollments = (
    df_enrollments
    .filter((F.col("term") != "2025T1") & (F.col("enrolled_at") >= F.lit("2025-01-13 11:00:00").cast("timestamp")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.date_trunc("day", F.col("enrolled_at")).alias("enrolled_at_day"),
    )
)

metrics = (
    prepared_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.ntile(5).over(w_all_by_score).alias("score_quintile"),
        F.round(F.col("score") - F.avg("score").over(w_term_course_id), 2).alias("score_vs_term_course_id_avg"),
        F.round(F.avg("score").over(w_all_by_score_moving_3), 2).alias("score_avg_4"),
        F.round(F.cume_dist().over(w_all_by_score), 3).alias("score_cume_dist"),
        F.lag("score").over(w_all_by_score).alias("prev_score"),
        F.min("score").over(w_term_course_id).alias("term_course_id_min_score"),
        F.dense_rank().over(w_term_by_enrolled_at_ties).alias("enrolled_at_dense_rank"),
        F.ntile(4).over(w_term_by_enrolled_at).alias("enrolled_at_quartile"),
        F.lead("enrolled_at", 3).over(w_all_by_score).alias("next3_enrolled_at"),
        F.count(F.lit(1)).over(w_term_course_id).alias("rows_in_term_course_id"),
        F.first("enrolled_at").over(w_all_by_score_full).alias("first_enrolled_at"),
        F.sum("score").over(w_all_by_score_prior_6).alias("score_sum_6_prior"),
        F.size(F.collect_set("student_id").over(w_term_course_id)).alias("distinct_student_id_in_term_course_id"),
    )
)

ranked = (
    metrics
    .select(
        "id",
        "term",
        "score",
        "enrolled_at",
        "score_vs_term_course_id_avg",
        "score_cume_dist",
        "prev_score",
        "rows_in_term_course_id",
        "score_sum_6_prior",
        "distinct_student_id_in_term_course_id",
        F.first("enrolled_at_quartile").over(w_course_id_by_first_enrolled).alias("first_enrolled_at_quartile"),
        F.count(F.lit(1)).over(w_enrolled_at_quartile_student).alias("rows_in_enrolled_at_quartile_student_id"),
        F.round(F.cume_dist().over(w_distinct_student_id_in_term), 3).alias("rows_in_term_course_id_cume_dist"),
        F.sum("prev_score").over(w_next3_enrolled_at_month_enro).alias("prev_score_sum_3"),
    )
)

result = (
    ranked
    .filter(F.col("rows_in_enrolled_at_quartile_student_id") >= 3)
    .select(
        "id",
        "term",
        "score",
        "score_vs_term_course_id_avg",
        "rows_in_term_course_id",
        "score_sum_6_prior",
        "distinct_student_id_in_term_course_id",
        "first_enrolled_at_quartile",
        "rows_in_enrolled_at_quartile_student_id",
        "prev_score_sum_3",
        F.abs(F.col("prev_score") - 32.9).alias("prev_score_distance"),
    )
    .orderBy(F.col("id").desc(), F.col("prev_score_distance").desc())
)
