from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term_by_enrolled_at = Window.partitionBy("term").orderBy("enrolled_at", "id")
w_all_by_enrolled_at_ties = Window.orderBy(F.col("enrolled_at").desc())
w_term = Window.partitionBy("term")
w_course_id_by_enrolled_at_run = Window.partitionBy("course_id").orderBy(F.col("enrolled_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_enrolled_at_moving_3 = Window.orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(-3, Window.currentRow)
w_course_id_by_enrolled_at = Window.partitionBy("course_id").orderBy(F.col("enrolled_at").desc_nulls_first(), "id")
w_all_by_enrolled_at_running = Window.orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_enrolled_at_full = Window.orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_id_by_enrolled_at_ful = Window.partitionBy("course_id").orderBy(F.col("enrolled_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_student_id_by_first_score = Window.partitionBy("student_id").orderBy("first_score", "id")
w_student_id_by_next_score_tie = Window.partitionBy("student_id").orderBy(F.col("next_score").desc())
w_student_id_by_first_score_fu = Window.partitionBy("student_id").orderBy("first_score", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_enrolled_at_seq = Window.partitionBy("enrolled_at_seq")
w_distinct_course_ids_by_avg_e = Window.partitionBy("distinct_course_ids").orderBy(F.col("avg_enrolled_at_rank_2").asc_nulls_last())
w_distinct_course_ids_last_enr = Window.partitionBy("distinct_course_ids", F.year("last_enrolled_at_month"))
w_distinct_course_ids_by_avg_e_2 = Window.partitionBy("distinct_course_ids").orderBy(F.col("avg_enrolled_at_rank_2").asc_nulls_last(), "last_enrolled_at_month")

filtered = (
    df_enrollments
    .filter((F.col("course_id") <= 9) & (F.col("grade_status") == "FAIL"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.lower(F.trim(F.col("grade_status"))).alias("grade_status_clean"),
    )
)

sequenced = (
    filtered
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status_clean",
        F.row_number().over(w_term_by_enrolled_at).alias("enrolled_at_seq"),
        F.rank().over(w_all_by_enrolled_at_ties).alias("enrolled_at_rank"),
        F.max("score").over(w_term).alias("term_max_score"),
        F.last("enrolled_at", ignorenulls=True).over(w_course_id_by_enrolled_at_run).alias("last_known_enrolled_at"),
        F.sum("score").over(w_all_by_enrolled_at_moving_3).alias("score_sum_4"),
        F.lead("score").over(w_course_id_by_enrolled_at).alias("next_score"),
        F.count(F.lit(1)).over(w_term).alias("rows_in_term"),
        F.sum("score").over(w_all_by_enrolled_at_running).alias("running_score"),
        F.first("score").over(w_all_by_enrolled_at_full).alias("first_score"),
        F.rank().over(w_course_id_by_enrolled_at).alias("enrolled_at_rank_2"),
        F.last("enrolled_at").over(w_course_id_by_enrolled_at_ful).alias("last_enrolled_at"),
    )
)

top_rows = (
    sequenced
    .filter(F.col("enrolled_at_rank") <= 5)
    .drop("enrolled_at_rank")
)

windowed = (
    top_rows
    .select(
        "*",
        F.ntile(4).over(w_student_id_by_first_score).alias("first_score_quartile"),
        F.dense_rank().over(w_student_id_by_next_score_tie).alias("next_score_dense_rank"),
        F.last("next_score").over(w_student_id_by_first_score_fu).alias("last_next_score"),
        F.count(F.lit(1)).over(w_enrolled_at_seq).alias("rows_in_enrolled_at_seq"),
        F.round(F.avg("next_score").over(w_enrolled_at_seq), 2).alias("enrolled_at_seq_avg_next_score"),
    )
)

summary = (
    windowed
    .filter(F.col("rows_in_enrolled_at_seq").between(4, 18))
    .groupBy(
        F.date_trunc("month", "last_enrolled_at").alias("last_enrolled_at_month"),
    )
    .agg(
        F.round(F.stddev("enrolled_at_rank_2"), 2).alias("stddev_enrolled_at_rank_2"),
        F.countDistinct("course_id").alias("distinct_course_ids"),
        F.round(F.sum("term_max_score") / F.nullif(F.sum("enrolled_at_rank_2"), F.lit(0)), 3).alias("term_max_score_to_enrolled_at_rank_2_ratio"),
        F.sum("last_next_score").alias("total_last_next_score"),
        F.round(F.avg("enrolled_at_rank_2"), 2).alias("avg_enrolled_at_rank_2"),
        F.percentile_approx("rows_in_term", 0.5).alias("median_rows_in_term"),
    )
)

ranked = (
    summary
    .select(
        "*",
        F.dense_rank().over(w_distinct_course_ids_by_avg_e).alias("avg_enrolled_at_rank_2_dense_rank"),
        F.round(F.col("median_rows_in_term") - F.avg("median_rows_in_term").over(w_distinct_course_ids_last_enr), 2).alias("median_rows_in_term_vs_distinct_course_ids_last_"),
        F.lag("total_last_next_score", 2).over(w_distinct_course_ids_by_avg_e_2).alias("prev2_total_last_next_score"),
        F.round(F.avg("total_last_next_score").over(w_distinct_course_ids_last_enr), 2).alias("distinct_course_ids_last_enrolled_at_month_year_"),
    )
)

result = (
    ranked
    .filter(F.col("term_max_score_to_enrolled_at_rank_2_ratio") > 3.5)
    .select(
        "last_enrolled_at_month",
        "stddev_enrolled_at_rank_2",
        "distinct_course_ids",
        "term_max_score_to_enrolled_at_rank_2_ratio",
        "total_last_next_score",
        "avg_enrolled_at_rank_2",
        "median_rows_in_term",
        "avg_enrolled_at_rank_2_dense_rank",
        "median_rows_in_term_vs_distinct_course_ids_last_",
        "prev2_total_last_next_score",
        "distinct_course_ids_last_enrolled_at_month_year_",
        F.date_format(F.col("last_enrolled_at_month"), "MMM yyyy").alias("last_enrolled_at_month_label"),
        F.coalesce(F.col("median_rows_in_term"), F.lit(0)).alias("median_rows_in_term_filled"),
    )
    .orderBy(
        F.col("avg_enrolled_at_rank_2").asc_nulls_last(),
        F.col("stddev_enrolled_at_rank_2").asc(),
        "last_enrolled_at_month",
    )
    .limit(5)
)
