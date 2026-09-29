from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_by_score_moving = Window.partitionBy("grade_status").orderBy(F.col("score").desc(), "id").rowsBetween(-6, Window.currentRow)
w_grade_status_by_score_moving_2 = Window.partitionBy("grade_status").orderBy(F.col("score").desc(), "id").rowsBetween(-2, Window.currentRow)
w_grade_status_by_score_full = Window.partitionBy("grade_status").orderBy(F.col("score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_enrolled_at_year = Window.partitionBy(F.year("enrolled_at"))
w_grade_status_by_score = Window.partitionBy("grade_status").orderBy(F.col("score").desc(), "id")
w_all_by_course_credits_runnin = Window.orderBy("course_credits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_course_credits_ties = Window.orderBy("course_credits")
w_all_by_course_credits = Window.orderBy("course_credits", "id")
w_course_id_by_course_credits = Window.partitionBy("course_id").orderBy(F.col("course_credits").desc_nulls_first())
w_grade_status_by_score_moving_3 = Window.partitionBy("grade_status").orderBy(F.col("score").desc(), "id").rowsBetween(-1, Window.currentRow)
w_all_by_days_since_first_enro = Window.orderBy("days_since_first_enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_days_since_first_enro_2 = Window.orderBy("days_since_first_enrolled_at")
w_all_by_days_since_first_enro_3 = Window.orderBy("days_since_first_enrolled_at", "id").rowsBetween(-6, Window.currentRow)
w_course_credits_rank_score_de = Window.partitionBy("course_credits_rank", "score_dense_rank")
w_student_id_enrolled_at_month = Window.partitionBy("student_id", F.month("enrolled_at")).orderBy(F.col("next2_score").desc(), "id")
w_first_enrolled_at_year_by_en = Window.partitionBy(F.year("first_enrolled_at")).orderBy("enrolled_at", "id")
w_score_dense_rank_by_max_firs = Window.partitionBy("score_dense_rank").orderBy(F.col("max_first_score_adj").desc(), "score_dense_rank", "next2_score_rank", "enrolled_at_month").rowsBetween(-1, Window.currentRow)
w_n_rows = Window.partitionBy("n_rows")
w_score_dense_rank_by_max_firs_2 = Window.partitionBy("score_dense_rank").orderBy(F.col("max_first_score_adj").desc(), "score_dense_rank", "next2_score_rank", "enrolled_at_month").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_enrollments
    .filter(F.col("enrolled_at") < F.lit("2025-02-18 10:00:00").cast("timestamp"))
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.date_format(F.col("enrolled_at"), "yyyy-MM-dd").alias("enrolled_at_label"),
        F.round(F.col("score") * 1.5, 2).alias("score_adj"),
    )
)

with_parent = (
    prepared.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        "s.enrolled_at_label",
        "s.score_adj",
        F.col("c.credits").alias("course_credits"),
    )
)

sequenced = (
    with_parent
    .select(
        "*",
        F.round(F.avg("score_adj").over(w_grade_status_by_score_moving), 2).alias("score_adj_avg_7"),
        F.sum("score_adj").over(w_grade_status_by_score_moving_2).alias("score_adj_sum_3"),
        F.first("enrolled_at").over(w_grade_status_by_score_full).alias("first_enrolled_at"),
        F.first("score_adj").over(w_grade_status_by_score_full).alias("first_score_adj"),
        F.size(F.collect_set("course_id").over(w_enrolled_at_year)).alias("distinct_course_id_in_enrolled_at_year"),
        F.dense_rank().over(w_grade_status_by_score).alias("score_dense_rank"),
        F.last("enrolled_at", ignorenulls=True).over(w_all_by_course_credits_runnin).alias("last_known_enrolled_at"),
        F.rank().over(w_all_by_course_credits_ties).alias("course_credits_rank"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_enrolled_at_year), F.lit(0)), 4).alias("score_share_of_enrolled_at_year"),
        F.count(F.lit(1)).over(w_enrolled_at_year).alias("rows_in_enrolled_at_year"),
        F.lead("score", 3).over(w_all_by_course_credits).alias("next3_score"),
        F.dense_rank().over(w_course_id_by_course_credits).alias("course_credits_dense_rank"),
        F.lead("score", 2, 0).over(w_all_by_course_credits).alias("next2_score"),
        F.round(F.avg("score_adj").over(w_grade_status_by_score_moving_3), 2).alias("score_adj_avg_2"),
        F.lead("score_adj", 2, 0).over(w_grade_status_by_score).alias("next2_score_adj"),
    )
)

scored = (
    sequenced
    .select(
        "id",
        "student_id",
        "score",
        "enrolled_at",
        "enrolled_at_label",
        "score_adj",
        "course_credits",
        "score_adj_avg_7",
        "score_adj_sum_3",
        "first_enrolled_at",
        "first_score_adj",
        "distinct_course_id_in_enrolled_at_year",
        "score_dense_rank",
        "course_credits_rank",
        "rows_in_enrolled_at_year",
        "next3_score",
        "next2_score",
        "score_adj_avg_2",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("first_enrolled_at")).alias("days_since_first_enrolled_at"),
        F.round((F.unix_timestamp(F.col("last_known_enrolled_at")) - F.unix_timestamp(F.col("enrolled_at"))) / 3600, 1).alias("hours_enrolled_at_to_last_known_enrolled_at"),
        F.concat(F.col("grade_status"), F.lit("#"), F.col("id").cast("string")).alias("grade_status_ref"),
    )
)

with_windows = (
    scored
    .select(
        "*",
        F.count(F.lit(1)).over(w_all_by_days_since_first_enro).alias("running_count"),
        F.round(F.percent_rank().over(w_all_by_days_since_first_enro_2), 3).alias("days_since_first_enrolled_at_percent_rank"),
        F.sum("score_adj").over(w_all_by_days_since_first_enro_3).alias("score_adj_sum_7"),
        F.size(F.collect_set("student_id").over(w_course_credits_rank_score_de)).alias("distinct_student_id_in_course_credits_rank_score"),
        F.rank().over(w_student_id_enrolled_at_month).alias("next2_score_rank"),
        F.sum("next2_score").over(w_course_credits_rank_score_de).alias("course_credits_rank_score_dense_rank_sum_next2_s"),
        F.round(F.cume_dist().over(w_first_enrolled_at_year_by_en), 3).alias("enrolled_at_cume_dist"),
    )
)

totals = (
    with_windows
    .groupBy(
        "score_dense_rank",
        "next2_score_rank",
        F.date_trunc("month", "enrolled_at").alias("enrolled_at_month"),
    )
    .agg(
        F.max("first_score_adj").alias("max_first_score_adj"),
        F.sum("running_count").alias("total_running_count"),
        F.max("enrolled_at").alias("last_enrolled_at"),
        F.count("*").alias("n_rows"),
    )
)

with_trends = (
    totals
    .select(
        "score_dense_rank",
        "next2_score_rank",
        "enrolled_at_month",
        "max_first_score_adj",
        "total_running_count",
        "last_enrolled_at",
        "n_rows",
        F.sum("next2_score_rank").over(w_score_dense_rank_by_max_firs).alias("next2_score_rank_sum_2"),
        F.count(F.lit(1)).over(w_score_dense_rank_by_max_firs_2).alias("running_count"),
        F.round(F.avg("total_running_count").over(w_n_rows), 2).alias("n_rows_avg_total_running_count"),
    )
)

result = (
    with_trends
    .filter(F.col("total_running_count") <= 66)
    .select(
        "score_dense_rank",
        "next2_score_rank",
        "max_first_score_adj",
        "total_running_count",
        "last_enrolled_at",
        "n_rows",
        "n_rows_avg_total_running_count",
        (F.col("n_rows") + F.col("running_count")).alias("n_rows_plus_running_count"),
    )
    .orderBy(
        F.col("next2_score_rank").asc_nulls_last(),
        F.col("n_rows_plus_running_count").desc(),
    )
)
