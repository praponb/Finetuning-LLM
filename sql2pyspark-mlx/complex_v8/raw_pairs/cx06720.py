from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term_course_id_by_enrolled_a = Window.partitionBy("term", "course_id").orderBy(F.col("enrolled_at").desc())
w_course_id_enrolled_at_month = Window.partitionBy("course_id", F.date_trunc("month", "enrolled_at"))
w_term = Window.partitionBy("term")
w_all_by_score_moving_1 = Window.orderBy(F.col("score").desc(), "id").rowsBetween(-1, Window.currentRow)
w_all_by_score = Window.orderBy(F.col("score").asc_nulls_last(), "id")
w_student_id = Window.partitionBy("student_id")
w_all_by_score_moving_3 = Window.orderBy(F.col("score").desc(), "id").rowsBetween(-3, Window.currentRow)
w_all_by_enrolled_at_dense_ran = Window.orderBy(F.col("enrolled_at_dense_rank").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_enrolled_at_month = Window.partitionBy("enrolled_at_month")

filtered_enrollments = (
    df_enrollments
    .filter(F.col("grade_status") == "FAIL")
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

ranked = (
    filtered_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.dense_rank().over(w_term_course_id_by_enrolled_a).alias("enrolled_at_dense_rank"),
        F.min("score").over(w_course_id_enrolled_at_month).alias("course_id_enrolled_at_month_min_score"),
        F.max("score").over(w_term).alias("term_max_score"),
        F.size(F.collect_set("course_id").over(w_term)).alias("distinct_course_id_in_term"),
        F.sum("score").over(w_term).alias("term_sum_score"),
        F.max("score").over(w_course_id_enrolled_at_month).alias("course_id_enrolled_at_month_max_score"),
        F.max("score").over(w_all_by_score_moving_1).alias("score_max_2"),
        F.rank().over(w_all_by_score).alias("score_rank"),
        F.round(F.col("score") - F.avg("score").over(w_term), 2).alias("score_vs_term_avg"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_course_id_enrolled_at_month), F.lit(0)), 4).alias("score_share_of_course_id_enrolled_at_month"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_student_id), F.lit(0)), 4).alias("score_share_of_student_id"),
        F.max("score").over(w_all_by_score_moving_3).alias("score_max_4"),
        F.sum("score").over(w_student_id).alias("student_id_sum_score"),
    )
)

enriched = (
    ranked
    .filter(F.col("student_id") >= 17)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        "enrolled_at_dense_rank",
        "course_id_enrolled_at_month_min_score",
        "term_max_score",
        "distinct_course_id_in_term",
        "score_max_2",
        "score_rank",
        "score_vs_term_avg",
        "score_share_of_course_id_enrolled_at_month",
        "score_share_of_student_id",
        "score_max_4",
        F.month(F.col("enrolled_at")).alias("enrolled_at_month"),
    )
)

sequenced = (
    enriched
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "enrolled_at",
        "enrolled_at_dense_rank",
        "course_id_enrolled_at_month_min_score",
        "term_max_score",
        "score_max_2",
        "score_vs_term_avg",
        "score_share_of_student_id",
        F.nth_value("score_max_2", 2).over(w_all_by_enrolled_at_dense_ran).alias("second_score_max_2"),
        F.count(F.lit(1)).over(w_enrolled_at_month).alias("rows_in_enrolled_at_month"),
        F.sum("score").over(w_enrolled_at_month).alias("enrolled_at_month_sum_score"),
    )
)

per_group = (
    sequenced
    .groupBy("student_id", "enrolled_at_dense_rank", "course_id")
    .agg(
        F.count(F.when(~F.col("term").isin("2024T2", "2024T1"), 1)).alias("n_term_not_in_2024t2_2024t1"),
        F.max("score_share_of_student_id").alias("max_score_share_of_student_id"),
        F.avg("score_share_of_student_id").alias("avg_score_share_of_student_id_check"),
    )
    .filter(F.col("avg_score_share_of_student_id_check") > 0.3)
    .drop("avg_score_share_of_student_id_check")
)

result = (
    per_group
    .filter(F.col("course_id") >= 5)
    .select(
        "student_id",
        "enrolled_at_dense_rank",
        "course_id",
        "n_term_not_in_2024t2_2024t1",
        "max_score_share_of_student_id",
    )
    .orderBy(
        F.col("student_id").desc(),
        F.col("max_score_share_of_student_id").asc_nulls_last(),
    )
)
