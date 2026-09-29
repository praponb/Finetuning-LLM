from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status = Window.partitionBy("grade_status")
w_grade_status_by_score_filled = Window.partitionBy("grade_status").orderBy("score_filled", "id")
w_all_by_enrolled_at = Window.orderBy(F.col("enrolled_at").desc(), "id")
w_grade_status_by_score_filled_2 = Window.partitionBy("grade_status").orderBy("score_filled", "id").rowsBetween(-2, -1)
w_all_by_enrolled_at_ties = Window.orderBy(F.col("enrolled_at").desc())
w_all_by_enrolled_at_moving_2 = Window.orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(-2, Window.currentRow)
w_grade_status_by_score_filled_3 = Window.partitionBy("grade_status").orderBy("score_filled")
w_grade_status_by_enrolled_at = Window.partitionBy("grade_status").orderBy(F.col("enrolled_at").desc(), "id")
w_score_filled_quartile = Window.partitionBy("score_filled_quartile")
w_enrolled_at_rank_by_avg_enro = Window.partitionBy("enrolled_at_rank").orderBy("avg_enrolled_at_percent_rank", "grade_status", "score_filled_quartile", "enrolled_at_rank")

filtered = (
    df_enrollments
    .filter((F.col("enrolled_at") < F.lit("2025-03-02 09:00:00").cast("timestamp")) & (F.col("student_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.substring(F.trim(F.col("term")), 1, 3).alias("term_prefix"),
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
    )
)

sequenced = (
    filtered
    .select(
        "*",
        F.min("score").over(w_grade_status).alias("grade_status_min_score"),
        F.ntile(4).over(w_grade_status_by_score_filled).alias("score_filled_quartile"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_grade_status), F.lit(0)), 4).alias("score_share_of_grade_status"),
        F.max("score_filled").over(w_grade_status).alias("grade_status_max_score_filled"),
        F.rank().over(w_all_by_enrolled_at).alias("enrolled_at_rank"),
        F.round(F.avg("score_filled").over(w_grade_status_by_score_filled_2), 2).alias("score_filled_avg_2_prior"),
        F.round(F.percent_rank().over(w_all_by_enrolled_at_ties), 3).alias("enrolled_at_percent_rank"),
        F.round(F.avg("score_filled").over(w_all_by_enrolled_at_moving_2), 2).alias("score_filled_avg_3"),
        F.size(F.collect_set("student_id").over(w_grade_status)).alias("distinct_student_id_in_grade_status"),
        F.round(F.cume_dist().over(w_grade_status_by_score_filled_3), 3).alias("score_filled_cume_dist"),
        F.round(F.percent_rank().over(w_all_by_enrolled_at), 3).alias("enrolled_at_percent_rank_2"),
        (F.col("score") - F.lag("score").over(w_all_by_enrolled_at)).alias("score_change"),
        F.round(F.cume_dist().over(w_grade_status_by_enrolled_at), 3).alias("enrolled_at_cume_dist"),
    )
)

top_per_group = sequenced.filter(F.col("enrolled_at_rank") == 1)

rollup_base = (
    top_per_group
    .groupBy("grade_status", "score_filled_quartile", "enrolled_at_rank")
    .agg(
        F.min("score_filled_quartile").alias("min_score_filled_quartile"),
        F.round(F.avg("enrolled_at_percent_rank"), 2).alias("avg_enrolled_at_percent_rank"),
        F.max("enrolled_at").alias("last_enrolled_at"),
        F.round(F.sum("score_filled_cume_dist"), 2).alias("total_score_filled_cume_dist"),
        F.round(F.sum("score_change") / F.nullif(F.sum("enrolled_at_percent_rank"), F.lit(0)), 3).alias("score_change_to_enrolled_at_percent_rank_ratio"),
    )
)

ranked = (
    rollup_base
    .withColumn("score_filled_quartile_sum_avg_enrolled_at_percen", F.sum("avg_enrolled_at_percent_rank").over(w_score_filled_quartile))
    .withColumn("avg_enrolled_at_percent_rank_cume_dist", F.round(F.cume_dist().over(w_enrolled_at_rank_by_avg_enro), 3))
)

result = (
    ranked
    .filter(F.col("grade_status").isNotNull())
    .select(
        "grade_status",
        "enrolled_at_rank",
        "min_score_filled_quartile",
        "avg_enrolled_at_percent_rank",
        "last_enrolled_at",
        F.year(F.col("last_enrolled_at")).alias("last_enrolled_at_year"),
    )
    .orderBy("avg_enrolled_at_percent_rank", F.col("last_enrolled_at").desc())
)
