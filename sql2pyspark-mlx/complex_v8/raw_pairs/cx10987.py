from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term_by_score = Window.partitionBy("term").orderBy("score", "id")
w_term_by_score_2 = Window.partitionBy("term").orderBy(F.col("score").desc(), "id")
w_term_by_score_ties = Window.partitionBy("term").orderBy(F.col("score").desc())
w_score_rank_2_score_quartile = Window.partitionBy("score_rank_2", "score_quartile")
w_enrolled_at_plus_15m_to_date = Window.partitionBy(F.to_date("enrolled_at_plus_15m")).orderBy("enrolled_at_plus_15m", "id")
w_score_quartile_by_enrolled_a = Window.partitionBy("score_quartile").orderBy("enrolled_at_plus_15m")

scoped_enrollments = (
    df_enrollments
    .filter(F.col("grade_status").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        (F.col("enrolled_at") + F.expr("INTERVAL 15 MINUTES")).alias("enrolled_at_plus_15m"),
        F.concat(F.col("term"), F.lit("#"), F.col("student_id").cast("string")).alias("term_ref"),
        F.substring(F.trim(F.col("term")), 1, 2).alias("term_prefix"),
    )
)

ranked = (
    scoped_enrollments
    .select(
        "id",
        "enrolled_at_plus_15m",
        "term_ref",
        "term_prefix",
        F.rank().over(w_term_by_score).alias("score_rank"),
        F.round(F.cume_dist().over(w_term_by_score), 3).alias("score_cume_dist"),
        F.ntile(4).over(w_term_by_score_2).alias("score_quartile"),
        F.rank().over(w_term_by_score_ties).alias("score_rank_2"),
    )
)

sequenced = (
    ranked
    .select(
        "id",
        "term_ref",
        "score_rank",
        "score_cume_dist",
        "score_rank_2",
        F.round(F.col("score_cume_dist") / F.nullif(F.sum("score_cume_dist").over(w_score_rank_2_score_quartile), F.lit(0)), 4).alias("score_cume_dist_share_of_score_rank_2_score_quar"),
        F.ntile(10).over(w_enrolled_at_plus_15m_to_date).alias("enrolled_at_plus_15m_decile"),
        F.round(F.percent_rank().over(w_score_quartile_by_enrolled_a), 3).alias("enrolled_at_plus_15m_percent_rank"),
        F.round(F.col("score_rank_2") - F.avg("score_rank_2").over(w_score_rank_2_score_quartile), 2).alias("score_rank_2_vs_score_rank_2_score_quartile_avg"),
        F.round(F.col("score_cume_dist") - F.avg("score_cume_dist").over(w_score_rank_2_score_quartile), 2).alias("score_cume_dist_vs_score_rank_2_score_quartile_a"),
    )
)

top_per_group = (
    sequenced
    .filter((F.col("score_rank_2") == 1) & (F.col("term_ref").isNotNull()))
    .drop("score_rank_2")
)

result = (
    top_per_group
    .groupingSets(
        [["enrolled_at_plus_15m_decile", "score_rank"], ["enrolled_at_plus_15m_decile"], ["score_rank"], []],
        "enrolled_at_plus_15m_decile", "score_rank",
    )
    .agg(
        F.percentile_approx("score_rank", 0.5).alias("median_score_rank"),
        F.count("*").alias("cnt"),
    )
    .orderBy(
        F.col("enrolled_at_plus_15m_decile").asc_nulls_last(),
        F.col("score_rank").asc_nulls_last(),
    )
)
