from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id_student_id = Window.partitionBy("course_id", "student_id")
w_grade_status_student_id_by_e = Window.partitionBy("grade_status", "student_id").orderBy(F.col("enrolled_at").desc())
w_term_by_enrolled_at = Window.partitionBy("term").orderBy(F.col("enrolled_at").desc(), "id")
w_term_by_enrolled_at_full = Window.partitionBy("term").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_score_full = Window.orderBy(F.col("score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_grade_status_student_id_by_e_2 = Window.partitionBy("grade_status", "student_id").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(-2, -1)
w_student_id_by_enrolled_at = Window.partitionBy("student_id").orderBy(F.col("enrolled_at").desc(), "id")
w_course_id = Window.partitionBy("course_id")
w_grade_status_student_id_by_e_3 = Window.partitionBy("grade_status", "student_id").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(-6, -1)
w_grade_status_student_id_by_e_4 = Window.partitionBy("grade_status", "student_id").orderBy(F.col("enrolled_at").desc(), "id")
w_all_by_score_prior_3 = Window.orderBy(F.col("score").desc(), "id").rowsBetween(-3, -1)
w_grade_status_student_id_by_e_5 = Window.partitionBy("grade_status", "student_id").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_enrolled_at = Window.orderBy(F.col("enrolled_at").desc(), "id")
w_all_by_enrolled_at_percent_r = Window.orderBy("enrolled_at_percent_rank_2", "id")
w_all_by_running_score_full = Window.orderBy(F.col("running_score").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_term_or_unknown_enrolled_at = Window.partitionBy("term_or_unknown", "enrolled_at_quartile")

prepared = (
    df_enrollments
    .filter(F.col("course_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("term"), F.lit("UNKNOWN")).alias("term_or_unknown"),
    )
)

windowed = (
    prepared
    .select(
        "*",
        F.count(F.lit(1)).over(w_course_id_student_id).alias("rows_in_course_id_student_id"),
        F.round(F.percent_rank().over(w_grade_status_student_id_by_e), 3).alias("enrolled_at_percent_rank"),
        F.round(F.percent_rank().over(w_term_by_enrolled_at), 3).alias("enrolled_at_percent_rank_2"),
        F.nth_value("score", 2).over(w_term_by_enrolled_at_full).alias("second_score"),
        F.last("enrolled_at").over(w_all_by_score_full).alias("last_enrolled_at"),
        F.round(F.avg("score").over(w_grade_status_student_id_by_e_2), 2).alias("score_avg_2_prior"),
        F.first("score").over(w_term_by_enrolled_at_full).alias("first_score"),
        F.rank().over(w_grade_status_student_id_by_e).alias("enrolled_at_rank"),
        F.ntile(4).over(w_student_id_by_enrolled_at).alias("enrolled_at_quartile"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_course_id), F.lit(0)), 4).alias("score_share_of_course_id"),
        F.round(F.cume_dist().over(w_student_id_by_enrolled_at), 3).alias("enrolled_at_cume_dist"),
        F.round(F.avg("score").over(w_grade_status_student_id_by_e_3), 2).alias("score_avg_6_prior"),
        F.ntile(10).over(w_grade_status_student_id_by_e_4).alias("enrolled_at_decile"),
        F.round(F.avg("score").over(w_all_by_score_prior_3), 2).alias("score_avg_3_prior"),
        F.sum("score").over(w_grade_status_student_id_by_e_5).alias("running_score"),
    )
)

scored = (
    windowed
    .select(
        "*",
        (F.floor(F.col("second_score") / 25) * 25).alias("second_score_bucket"),
    )
)

sequenced = (
    scored
    .select(
        "id",
        "student_id",
        "term",
        "score",
        "enrolled_at",
        "term_or_unknown",
        "rows_in_course_id_student_id",
        "second_score",
        "last_enrolled_at",
        "first_score",
        "enrolled_at_rank",
        "score_share_of_course_id",
        "enrolled_at_cume_dist",
        "score_avg_6_prior",
        "enrolled_at_decile",
        "score_avg_3_prior",
        "running_score",
        "second_score_bucket",
        F.rank().over(w_all_by_enrolled_at).alias("enrolled_at_rank_2"),
        F.row_number().over(w_all_by_enrolled_at_percent_r).alias("seq_no"),
        F.last("running_score").over(w_all_by_running_score_full).alias("last_running_score"),
        F.round(F.col("score_avg_2_prior") - F.avg("score_avg_2_prior").over(w_term_or_unknown_enrolled_at), 2).alias("score_avg_2_prior_vs_term_or_unknown_enrolled_at"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "student_id",
        "term",
        "enrolled_at",
        "rows_in_course_id_student_id",
        "last_enrolled_at",
        "enrolled_at_rank",
        "score_share_of_course_id",
        "score_avg_6_prior",
        "score_avg_3_prior",
        "running_score",
        "second_score_bucket",
        "enrolled_at_rank_2",
        "last_running_score",
        "score_avg_2_prior_vs_term_or_unknown_enrolled_at",
        F.when(F.col("score") >= 59.8, "HIGH").when(F.col("score") >= 26.2, "MEDIUM").otherwise("LOW").alias("score_tier"),
        F.coalesce(F.col("enrolled_at_rank_2"), F.lit(0)).alias("enrolled_at_rank_2_filled"),
    )
)
