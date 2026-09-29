from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_score_tier_by_enrolled_at_pr = Window.partitionBy("score_tier").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(-1, -1)
w_course_id_student_cohort_by = Window.partitionBy("course_id", "student_cohort").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_student_id_score_tier_by_sco = Window.partitionBy("student_id", "score_tier").orderBy(F.col("score").desc(), "id")
w_student_cohort = Window.partitionBy("student_cohort")
w_course_id_student_cohort_by_2 = Window.partitionBy("course_id", "student_cohort").orderBy(F.col("enrolled_at").desc(), "id")
w_score_tier_by_enrolled_at_ti = Window.partitionBy("score_tier").orderBy(F.col("enrolled_at").desc())
w_enrolled_at_rank_by_score_sh = Window.partitionBy("enrolled_at_rank").orderBy("score_share_of_student_cohort", "id")
w_score_tier = Window.partitionBy("score_tier")
w_all_by_score_avg_1_prior = Window.orderBy(F.col("score_avg_1_prior").desc(), "id")

prepared_enrollments = (
    df_enrollments
    .filter(F.col("student_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.when(F.col("score") >= 57.6, "large").when(F.col("score") >= 42.4, "medium").otherwise("small").alias("score_tier"),
    )
)

with_parent = (
    prepared_enrollments.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (F.col("st.major").isNotNull()), "inner")
    .select("s.*", F.col("st.cohort").alias("student_cohort"))
)

ranked = (
    with_parent
    .select(
        "*",
        F.round(F.avg("score").over(w_score_tier_by_enrolled_at_pr), 2).alias("score_avg_1_prior"),
        F.last("enrolled_at").over(w_course_id_student_cohort_by).alias("last_enrolled_at"),
        F.ntile(5).over(w_student_id_score_tier_by_sco).alias("score_quintile"),
        F.size(F.collect_set("grade_status").over(w_student_cohort)).alias("distinct_grade_status_in_student_cohort"),
        F.max("score").over(w_student_cohort).alias("student_cohort_max_score"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_student_cohort), F.lit(0)), 4).alias("score_share_of_student_cohort"),
        F.round(F.cume_dist().over(w_course_id_student_cohort_by_2), 3).alias("enrolled_at_cume_dist"),
        F.round(F.col("score") - F.avg("score").over(w_student_cohort), 2).alias("score_vs_student_cohort_avg"),
        F.rank().over(w_score_tier_by_enrolled_at_ti).alias("enrolled_at_rank"),
    )
)

top_per_group = ranked.filter(F.col("enrolled_at_rank") <= 3)

sequenced = (
    top_per_group
    .select(
        "id",
        "student_id",
        "score",
        "grade_status",
        "student_cohort",
        "score_share_of_student_cohort",
        "enrolled_at_cume_dist",
        "enrolled_at_rank",
        F.dense_rank().over(w_enrolled_at_rank_by_score_sh).alias("score_share_of_student_cohort_dense_rank"),
        F.round(F.col("score_quintile") / F.nullif(F.sum("score_quintile").over(w_score_tier), F.lit(0)), 4).alias("score_quintile_share_of_score_tier"),
        F.round(F.col("enrolled_at_rank") / F.nullif(F.sum("enrolled_at_rank").over(w_score_tier), F.lit(0)), 4).alias("enrolled_at_rank_share_of_score_tier"),
        F.round(F.col("score_quintile") - F.avg("score_quintile").over(w_score_tier), 2).alias("score_quintile_vs_score_tier_avg"),
        F.ntile(4).over(w_all_by_score_avg_1_prior).alias("score_avg_1_prior_quartile"),
        F.round((F.unix_timestamp("last_enrolled_at") - F.unix_timestamp(F.lag("last_enrolled_at").over(w_all_by_score_avg_1_prior))) / 60, 1).alias("minutes_since_prev_last_enrolled_at"),
    )
)

stats = (
    sequenced
    .filter(F.col("score_share_of_student_cohort_dense_rank") > 2)
    .groupBy(
        F.when(F.col("enrolled_at_cume_dist") >= 0.7, "HIGH").when(F.col("enrolled_at_cume_dist") >= 0.2, "MEDIUM").otherwise("LOW").alias("enrolled_at_cume_dist_tier"),
    )
    .agg(
        F.round(F.sum("score") / F.nullif(F.sum("enrolled_at_rank_share_of_score_tier"), F.lit(0)), 3).alias("score_to_enrolled_at_rank_share_of_score_tier_ra"),
        F.count("*").alias("n_rows"),
        F.array_sort(F.collect_set("student_cohort")).alias("student_cohort_list"),
        F.countDistinct("student_cohort").alias("distinct_student_cohorts"),
        F.count(F.when(F.col("grade_status").isNotNull(), 1)).alias("n_grade_status_is_not_null"),
    )
    .filter(F.col("n_rows") >= 2)
)

result = (
    stats
    .select(
        "enrolled_at_cume_dist_tier",
        "score_to_enrolled_at_rank_share_of_score_tier_ra",
        "n_rows",
        "distinct_student_cohorts",
        "n_grade_status_is_not_null",
        F.coalesce(F.col("distinct_student_cohorts"), F.lit(0)).alias("distinct_student_cohorts_filled"),
    )
    .orderBy(
        F.col("enrolled_at_cume_dist_tier").desc_nulls_first(),
        F.col("distinct_student_cohorts").desc(),
    )
)
