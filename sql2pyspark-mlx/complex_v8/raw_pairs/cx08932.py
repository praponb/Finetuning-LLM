from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_student_id_by_s = Window.partitionBy("grade_status", "student_id").orderBy("score", "id")
w_all_by_score_bucket = Window.orderBy("score_bucket", "id")
w_course_id_term = Window.partitionBy("course_id", "term")
w_term_by_score_ties = Window.partitionBy("term").orderBy(F.col("score").asc_nulls_last())
w_all_by_score_bucket_running = Window.orderBy("score_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grade_status_by_score_moving = Window.partitionBy("grade_status").orderBy(F.col("score").desc(), "id").rowsBetween(-2, Window.currentRow)
w_grade_status_by_score = Window.partitionBy("grade_status").orderBy(F.col("score").desc(), "id")
w_all_by_score_bucket_prior_2 = Window.orderBy("score_bucket", "id").rowsBetween(-2, -1)
w_term_by_score = Window.partitionBy("term").orderBy(F.col("score").asc_nulls_last(), "id")
w_score_bucket_dense_rank_grad = Window.partitionBy("score_bucket_dense_rank", "grade_status")
w_course_id_score_tier_by_scor = Window.partitionBy("course_id", "score_tier").orderBy("score_bucket_vs_course_id_term_avg", "id").rowsBetween(-1, -1)
w_all_by_prev_score_ties = Window.orderBy("prev_score")
w_score_bucket_dense_rank_by_a = Window.partitionBy("score_bucket_dense_rank").orderBy("avg_min_score_so_far", "score_bucket_dense_rank", "score_tier")
w_score_bucket_dense_rank_dist = Window.partitionBy("score_bucket_dense_rank", "distinct_student_ids")

filtered_enrollments = (
    df_enrollments
    .filter((F.col("score") <= 80.2) & (F.col("enrolled_at") >= F.lit("2025-01-31 14:00:00").cast("timestamp")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
        F.when(F.col("score") >= 57.6, "A").when(F.col("score") >= 42.4, "B").otherwise("C").alias("score_tier"),
    )
)

with_trends = (
    filtered_enrollments
    .select(
        "*",
        F.row_number().over(w_grade_status_student_id_by_s).alias("seq_no"),
        F.lag("score", 1, 0).over(w_all_by_score_bucket).alias("prev_score"),
        F.round(F.col("score_bucket") / F.nullif(F.sum("score_bucket").over(w_course_id_term), F.lit(0)), 4).alias("score_bucket_share_of_course_id_term"),
        F.round(F.col("score_bucket") - F.avg("score_bucket").over(w_course_id_term), 2).alias("score_bucket_vs_course_id_term_avg"),
        F.round(F.percent_rank().over(w_term_by_score_ties), 3).alias("score_percent_rank"),
        F.min("score").over(w_all_by_score_bucket_running).alias("min_score_so_far"),
        F.max("score").over(w_grade_status_by_score_moving).alias("score_max_3"),
        F.ntile(10).over(w_grade_status_by_score).alias("score_decile"),
        F.sum("score").over(w_all_by_score_bucket_prior_2).alias("score_sum_2_prior"),
        F.dense_rank().over(w_all_by_score_bucket).alias("score_bucket_dense_rank"),
        F.row_number().over(w_term_by_score).alias("seq_no_2"),
    )
)

with_trends_2 = (
    with_trends
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "score_bucket",
        "score_tier",
        "prev_score",
        "score_bucket_share_of_course_id_term",
        "score_bucket_vs_course_id_term_avg",
        "score_percent_rank",
        "min_score_so_far",
        "score_max_3",
        "score_decile",
        "score_sum_2_prior",
        "score_bucket_dense_rank",
        "seq_no_2",
        F.round(F.col("prev_score") / F.nullif(F.sum("prev_score").over(w_score_bucket_dense_rank_grad), F.lit(0)), 4).alias("prev_score_share_of_score_bucket_dense_rank_grad"),
        F.max("seq_no").over(w_course_id_score_tier_by_scor).alias("seq_no_max_1_prior"),
        F.round(F.cume_dist().over(w_all_by_prev_score_ties), 3).alias("prev_score_cume_dist"),
        F.round(F.col("score_bucket") / F.nullif(F.sum("score_bucket").over(w_score_bucket_dense_rank_grad), F.lit(0)), 4).alias("score_bucket_share_of_score_bucket_dense_rank_gr"),
    )
)

rollup_base = (
    with_trends_2
    .groupBy("score_bucket_dense_rank", "score_tier")
    .agg(
        F.count("*").alias("row_count"),
        F.sum("prev_score").alias("total_prev_score"),
        F.countDistinct("student_id").alias("distinct_student_ids"),
        F.round(F.avg("min_score_so_far"), 2).alias("avg_min_score_so_far"),
    )
)

sequenced = (
    rollup_base
    .select(
        "score_bucket_dense_rank",
        "score_tier",
        "row_count",
        (F.col("score_bucket_dense_rank") - F.lag("score_bucket_dense_rank").over(w_score_bucket_dense_rank_by_a)).alias("score_bucket_dense_rank_change"),
        F.max("score_bucket_dense_rank").over(w_score_bucket_dense_rank_dist).alias("score_bucket_dense_rank_distinct_student_ids_max"),
        F.min("score_bucket_dense_rank").over(w_score_bucket_dense_rank_dist).alias("score_bucket_dense_rank_distinct_student_ids_min"),
    )
)

result = (
    sequenced
    .select(
        "score_bucket_dense_rank",
        "score_tier",
        "score_bucket_dense_rank_distinct_student_ids_max",
        "score_bucket_dense_rank_distinct_student_ids_min",
    )
    .orderBy(
        F.col("score_tier").desc(),
        F.col("score_bucket_dense_rank_distinct_student_ids_max").asc(),
    )
)
