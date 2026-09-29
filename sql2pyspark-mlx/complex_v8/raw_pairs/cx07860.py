from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_enrolled_at_year_year_distin = Window.partitionBy("enrolled_at_year_year", "distinct_ids").orderBy(F.col("distinct_ids").asc_nulls_last(), "enrolled_at_ym", "enrolled_at_year_year").rowsBetween(-2, -1)
w_enrolled_at_year_year = Window.partitionBy("enrolled_at_year_year")
w_enrolled_at_year_year_distin_2 = Window.partitionBy("enrolled_at_year_year", "distinct_ids").orderBy(F.col("distinct_ids").asc_nulls_last(), "enrolled_at_ym", "enrolled_at_year_year")
w_all_by_rn_ties = Window.orderBy(F.col("rn").desc())
w_all_by_rn = Window.orderBy(F.col("rn").desc(), "enrolled_at_ym", "enrolled_at_year_year")
w_all_by_rn_running = Window.orderBy(F.col("rn").desc(), "enrolled_at_ym", "enrolled_at_year_year").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_distinct_ids_cnt = Window.partitionBy("distinct_ids", "cnt")

valid_enrollments = (
    df_enrollments
    .filter((F.col("course_id") <= 5) & (F.col("term").isin("2025T1", "2024T1")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
    )
)

with_activity = (
    valid_enrollments.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.student_id").isNotNull()) & (F.col("e.score") >= 8.7), "left_anti")
)

annotated = (
    with_activity
    .filter(F.col("course_id") >= 4)
    .select(
        "id",
        "course_id",
        "term",
        "enrolled_at",
        F.date_trunc("year", F.col("enrolled_at")).alias("enrolled_at_year"),
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
    )
)

summary = (
    annotated
    .groupBy(
        F.date_format("enrolled_at", "yyyy-MM").alias("enrolled_at_ym"),
        F.year("enrolled_at_year").alias("enrolled_at_year_year"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("term").alias("distinct_terms"),
        F.min("score_bucket").alias("min_score_bucket"),
    )
)

windowed = (
    summary
    .select(
        "enrolled_at_ym",
        "enrolled_at_year_year",
        "cnt",
        "distinct_ids",
        "min_score_bucket",
        F.round(F.avg("enrolled_at_year_year").over(w_enrolled_at_year_year_distin), 2).alias("enrolled_at_year_year_avg_2_prior"),
        F.max("distinct_terms").over(w_enrolled_at_year_year).alias("enrolled_at_year_year_max_distinct_terms"),
        F.row_number().over(w_enrolled_at_year_year_distin_2).alias("rn"),
    )
)

metrics = (
    windowed
    .select(
        "enrolled_at_ym",
        "enrolled_at_year_year",
        "cnt",
        "distinct_ids",
        "min_score_bucket",
        "enrolled_at_year_year_avg_2_prior",
        "enrolled_at_year_year_max_distinct_terms",
        "rn",
        F.dense_rank().over(w_all_by_rn_ties).alias("rn_dense_rank"),
        F.lag("enrolled_at_year_year").over(w_all_by_rn).alias("prev_enrolled_at_year_year"),
        F.count(F.lit(1)).over(w_all_by_rn_running).alias("running_count"),
        F.dense_rank().over(w_all_by_rn).alias("rn_dense_rank_2"),
        F.rank().over(w_all_by_rn).alias("rn_rank"),
        F.ntile(5).over(w_all_by_rn).alias("rn_quintile"),
    )
)

result = (
    metrics
    .select(
        "enrolled_at_ym",
        "enrolled_at_year_year",
        "cnt",
        "min_score_bucket",
        "enrolled_at_year_year_max_distinct_terms",
        "rn",
        "rn_dense_rank",
        "prev_enrolled_at_year_year",
        "running_count",
        "rn_dense_rank_2",
        "rn_rank",
        F.coalesce(F.col("rn_dense_rank"), F.lit(0)).alias("rn_dense_rank_filled"),
    )
    .orderBy(F.col("enrolled_at_ym").asc())
)
