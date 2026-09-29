from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("course_id").orderBy(F.col("score").desc(), F.col("id").desc())
w_min_term_len_term_by_n_rows = Window.partitionBy("min_term_len", "term").orderBy(F.col("n_rows").asc_nulls_last(), "term").rowsBetween(-3, Window.currentRow)
w_min_term_len_term_by_n_rows_2 = Window.partitionBy("min_term_len", "term").orderBy(F.col("n_rows").asc_nulls_last())
w_n_rows_by_min_term_len = Window.partitionBy("n_rows").orderBy("min_term_len", "term")
w_all_by_min_term_len_prior_6 = Window.orderBy(F.col("min_term_len").desc(), "term").rowsBetween(-6, -1)
w_all_by_min_term_len = Window.orderBy(F.col("min_term_len").desc(), "term")

valid_enrollments = (
    df_enrollments
    .filter((F.col("score") > 32.9) & (F.col("course_id") >= 9))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
        F.length(F.trim(F.col("term"))).alias("term_len"),
    )
)

latest = (
    valid_enrollments
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

stats = (
    latest
    .groupBy("term")
    .agg(
        F.min("term_len").alias("min_term_len"),
        F.count("*").alias("n_rows"),
        F.countDistinct("grade_status").alias("distinct_grade_status"),
        F.sum("score_filled").alias("sum_score_filled_check"),
    )
    .filter(F.col("sum_score_filled_check") > 30.0)
    .drop("sum_score_filled_check")
)

qualified = (
    stats
    .crossJoin(stats.agg(F.max("n_rows").alias("overall_max_n_rows")))
    .filter(F.col("n_rows") < F.col("overall_max_n_rows"))
    .drop("overall_max_n_rows")
)

metrics = (
    qualified
    .select(
        "*",
        F.sum("min_term_len").over(w_min_term_len_term_by_n_rows).alias("min_term_len_sum_4"),
        F.round(F.cume_dist().over(w_min_term_len_term_by_n_rows_2), 3).alias("n_rows_cume_dist"),
        (F.col("min_term_len") - F.lag("min_term_len").over(w_n_rows_by_min_term_len)).alias("min_term_len_change"),
    )
)

annotated = (
    metrics
    .select(
        "term",
        "min_term_len",
        "distinct_grade_status",
        "min_term_len_sum_4",
        "n_rows_cume_dist",
        F.concat(F.col("term"), F.lit("#"), F.col("n_rows").cast("string")).alias("term_ref"),
    )
)

metrics_2 = (
    annotated
    .select(
        "*",
        F.round(F.avg("n_rows_cume_dist").over(w_all_by_min_term_len_prior_6), 2).alias("n_rows_cume_dist_avg_6_prior"),
        (F.col("n_rows_cume_dist") - F.lag("n_rows_cume_dist").over(w_all_by_min_term_len)).alias("n_rows_cume_dist_change"),
        F.dense_rank().over(w_all_by_min_term_len).alias("min_term_len_dense_rank"),
    )
)

result = (
    metrics_2
    .select("term", "min_term_len", "distinct_grade_status")
    .unpivot(
        ids=["term"],
        values=["min_term_len", "distinct_grade_status"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
