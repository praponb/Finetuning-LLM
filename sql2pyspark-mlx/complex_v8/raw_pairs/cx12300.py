from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_enrolled_at_month = Window.partitionBy(F.date_trunc("month", "enrolled_at"))
w_course_id_term_by_enrolled_a = Window.partitionBy("course_id", "term").orderBy(F.col("enrolled_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_term_grade_status_by_enrolle = Window.partitionBy("term", "grade_status").orderBy(F.col("enrolled_at").asc_nulls_last(), "id")
w_term_grade_status_by_enrolle_2 = Window.partitionBy("term", "grade_status").orderBy("enrolled_at")
w_grade_status_course_id = Window.partitionBy("grade_status", "course_id")
w_term_grade_status_by_enrolle_3 = Window.partitionBy("term", "grade_status").orderBy(F.col("enrolled_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_id_term_by_enrolled_a_2 = Window.partitionBy("course_id", "term").orderBy(F.col("enrolled_at").asc_nulls_last(), "id")
w_term_grade_status_by_enrolle_4 = Window.partitionBy("term", "grade_status").orderBy("enrolled_at", "id").rowsBetween(-6, Window.currentRow)
w_term_grade_status_by_enrolle_5 = Window.partitionBy("term", "grade_status").orderBy(F.col("enrolled_at").asc_nulls_last())
w_last_known_score_tier_by_n_g = Window.partitionBy("last_known_score_tier").orderBy("n_grade_status_not_in_incomple")
w_last_known_score_tier_by_n_g_2 = Window.partitionBy("last_known_score_tier").orderBy("n_grade_status_not_in_incomple", "last_known_score_tier", "term", "course_id")

scoped = (
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
    )
)

sequenced = (
    scoped
    .select(
        "*",
        F.size(F.collect_set("course_id").over(w_enrolled_at_month)).alias("distinct_course_id_in_enrolled_at_month"),
        F.last("score", ignorenulls=True).over(w_course_id_term_by_enrolled_a).alias("last_known_score"),
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_term_grade_status_by_enrolle))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
        F.dense_rank().over(w_term_grade_status_by_enrolle_2).alias("enrolled_at_dense_rank"),
        F.size(F.collect_set("grade_status").over(w_enrolled_at_month)).alias("distinct_grade_status_in_enrolled_at_month"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_grade_status_course_id), F.lit(0)), 4).alias("score_share_of_grade_status_course_id"),
        F.first("enrolled_at").over(w_term_grade_status_by_enrolle_3).alias("first_enrolled_at"),
        F.round(F.col("score") - F.avg("score").over(w_enrolled_at_month), 2).alias("score_vs_enrolled_at_month_avg"),
        F.lead("enrolled_at").over(w_course_id_term_by_enrolled_a_2).alias("next_enrolled_at"),
        F.round(F.avg("score").over(w_grade_status_course_id), 2).alias("grade_status_course_id_avg_score"),
        F.max("score").over(w_term_grade_status_by_enrolle_4).alias("score_max_7"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_enrolled_at_month), F.lit(0)), 4).alias("score_share_of_enrolled_at_month"),
        F.dense_rank().over(w_term_grade_status_by_enrolle_5).alias("enrolled_at_dense_rank_2"),
    )
)

tagged = (
    sequenced
    .filter(F.col("enrolled_at") < F.lit("2025-02-18 11:00:00").cast("timestamp"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        "distinct_course_id_in_enrolled_at_month",
        "last_known_score",
        "minutes_since_prev_enrolled_at",
        "first_enrolled_at",
        "score_vs_enrolled_at_month_avg",
        "next_enrolled_at",
        "grade_status_course_id_avg_score",
        "score_max_7",
        "score_share_of_enrolled_at_month",
        "enrolled_at_dense_rank_2",
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
        F.coalesce(F.col("score_max_7"), F.lit(0.0)).alias("score_max_7_filled"),
    )
)

totals = (
    tagged
    .groupBy(
        F.when(F.col("last_known_score") >= 78.8, "HIGH").when(F.col("last_known_score") >= 32.9, "MEDIUM").otherwise("LOW").alias("last_known_score_tier"),
        "term",
        "course_id",
    )
    .agg(
        F.min("enrolled_at").alias("first_enrolled_at_2"),
        F.count(F.when(~F.col("grade_status").isin("INCOMPLETE", "FAIL"), 1)).alias("n_grade_status_not_in_incomple"),
    )
)

sequenced_2 = (
    totals
    .select(
        "last_known_score_tier",
        "term",
        "course_id",
        "first_enrolled_at_2",
        "n_grade_status_not_in_incomple",
        F.round(F.percent_rank().over(w_last_known_score_tier_by_n_g), 3).alias("n_grade_status_not_in_incomple_percent_rank"),
        F.round((F.unix_timestamp("first_enrolled_at_2") - F.unix_timestamp(F.lag("first_enrolled_at_2").over(w_last_known_score_tier_by_n_g_2))) / 60, 1).alias("minutes_since_prev_first_enrolled_at_2"),
        F.lag("first_enrolled_at_2", 3).over(w_last_known_score_tier_by_n_g_2).alias("prev3_first_enrolled_at_2"),
        F.dense_rank().over(w_last_known_score_tier_by_n_g).alias("n_grade_status_not_in_incomple_dense_rank"),
    )
)

result = (
    sequenced_2
    .select(
        "last_known_score_tier",
        "term",
        "course_id",
        "first_enrolled_at_2",
        "n_grade_status_not_in_incomple_percent_rank",
        "minutes_since_prev_first_enrolled_at_2",
    )
    .orderBy("last_known_score_tier", F.col("course_id").asc_nulls_last())
)
