from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_term_by_score = Window.partitionBy("grade_status", "term").orderBy("score", "id")
w_enrolled_at_to_date_course_i = Window.partitionBy(F.to_date("enrolled_at"), "course_id").orderBy(F.col("days_since_enrolled_at").desc_nulls_first(), "id").rowsBetween(-2, -1)
w_course_id = Window.partitionBy("course_id")
w_student_id_course_id = Window.partitionBy("student_id", "course_id")
w_all_by_score_prior_1 = Window.orderBy("score", "id").rowsBetween(-1, -1)
w_all_by_score_ties = Window.orderBy("score")
w_all_by_score = Window.orderBy("score", "id")
w_all_by_score_moving_6 = Window.orderBy("score", "id").rowsBetween(-6, Window.currentRow)
w_term_grade_status_by_score_b = Window.partitionBy("term", "grade_status").orderBy(F.col("score_bucket").desc(), "id")
w_grade_status_term_by_score_t = Window.partitionBy("grade_status", "term").orderBy("score")
w_distinct_grade_status_in_stu = Window.partitionBy("distinct_grade_status_in_student_id_course_id", "term")
w_all_by_stddev_score_sum_2_pr = Window.orderBy(F.col("stddev_score_sum_2_prior").desc_nulls_first(), "term", "distinct_grade_status_in_student_id_course_id")
w_all_by_stddev_score_sum_2_pr_2 = Window.orderBy(F.col("stddev_score_sum_2_prior").desc_nulls_first(), "term", "distinct_grade_status_in_student_id_course_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered_enrollments = (
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
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
    )
)

windowed = (
    filtered_enrollments
    .select(
        "*",
        F.row_number().over(w_grade_status_term_by_score).alias("seq_no"),
        F.sum("score").over(w_enrolled_at_to_date_course_i).alias("score_sum_2_prior"),
        F.round(F.col("days_since_enrolled_at") - F.avg("days_since_enrolled_at").over(w_course_id), 2).alias("days_since_enrolled_at_vs_course_id_avg"),
        F.size(F.collect_set("grade_status").over(w_student_id_course_id)).alias("distinct_grade_status_in_student_id_course_id"),
        F.sum("days_since_enrolled_at").over(w_all_by_score_prior_1).alias("days_since_enrolled_at_sum_1_prior"),
        F.rank().over(w_all_by_score_ties).alias("score_rank"),
        F.round(F.cume_dist().over(w_all_by_score), 3).alias("score_cume_dist"),
        F.sum("days_since_enrolled_at").over(w_all_by_score_moving_6).alias("days_since_enrolled_at_sum_7"),
        F.max("days_since_enrolled_at").over(w_student_id_course_id).alias("student_id_course_id_max_days_since_enrolled_at"),
        F.lead("score_bucket").over(w_term_grade_status_by_score_b).alias("next_score_bucket"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_student_id_course_id), F.lit(0)), 4).alias("score_share_of_student_id_course_id"),
        F.dense_rank().over(w_grade_status_term_by_score_t).alias("score_dense_rank"),
        F.ntile(10).over(w_term_grade_status_by_score_b).alias("score_bucket_decile"),
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_term_grade_status_by_score_b))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
        F.round(F.avg("score_bucket").over(w_student_id_course_id), 2).alias("student_id_course_id_avg_score_bucket"),
    )
)

enriched = (
    windowed
    .withColumn("grade_status_ref", F.concat(F.col("grade_status"), F.lit("#"), F.col("score_bucket").cast("string")))
)

summary = (
    enriched
    .groupBy("term", "distinct_grade_status_in_student_id_course_id")
    .agg(
        F.sum("days_since_enrolled_at_sum_7").alias("total_days_since_enrolled_at_sum_7"),
        F.count(F.when(F.col("term").isNotNull(), 1)).alias("n_term_is_not_null"),
        F.round(F.stddev("score_sum_2_prior"), 2).alias("stddev_score_sum_2_prior"),
        F.round(F.avg("minutes_since_prev_enrolled_at"), 2).alias("avg_minutes_since_prev_enrolled_at"),
    )
)

windowed_2 = (
    summary
    .select(
        "*",
        F.round(F.col("n_term_is_not_null") - F.avg("n_term_is_not_null").over(w_distinct_grade_status_in_stu), 2).alias("n_term_is_not_null_vs_distinct_grade_status_in_s"),
        F.row_number().over(w_all_by_stddev_score_sum_2_pr).alias("seq_no"),
        F.lead("stddev_score_sum_2_prior", 3, 0).over(w_all_by_stddev_score_sum_2_pr).alias("next3_stddev_score_sum_2_prior"),
        F.nth_value("n_term_is_not_null", 2).over(w_all_by_stddev_score_sum_2_pr_2).alias("second_n_term_is_not_null"),
    )
)

result = (
    windowed_2
    .select(
        "term",
        "distinct_grade_status_in_student_id_course_id",
        "total_days_since_enrolled_at_sum_7",
        "stddev_score_sum_2_prior",
        "avg_minutes_since_prev_enrolled_at",
        "next3_stddev_score_sum_2_prior",
        "second_n_term_is_not_null",
        F.when(F.col("term").isin("2024T2", "2025T1"), "group_a").when(F.col("term") == "2025T2", "group_b").otherwise("other").alias("term_group"),
    )
    .orderBy(F.col("avg_minutes_since_prev_enrolled_at").desc_nulls_first())
)
