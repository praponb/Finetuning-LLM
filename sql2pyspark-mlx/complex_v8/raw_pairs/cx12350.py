from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_credits_by_credits = Window.partitionBy("credits").orderBy("credits", "id")
w_level_credits = Window.partitionBy("level", "credits")
w_credits_dense_rank_credits = Window.partitionBy("credits_dense_rank", "credits")

prepared = (
    df_courses
    .filter(F.col("credits") > 2)
    .select(
        "id",
        "course_code",
        "credits",
        "level",
    )
)

with_windows = (
    prepared
    .select(
        "id",
        "credits",
        F.round(F.cume_dist().over(w_credits_by_credits), 3).alias("credits_cume_dist"),
        F.sum("credits").over(w_level_credits).alias("level_credits_sum_credits"),
        F.dense_rank().over(w_credits_by_credits).alias("credits_dense_rank"),
        F.min("credits").over(w_level_credits).alias("level_credits_min_credits"),
        F.row_number().over(w_credits_by_credits).alias("seq_no"),
        F.count(F.lit(1)).over(w_level_credits).alias("rows_in_level_credits"),
    )
)

windowed = (
    with_windows
    .select(
        "*",
        F.sum("credits").over(w_credits_dense_rank_credits).alias("credits_dense_rank_credits_sum_credits"),
        F.min("credits").over(w_credits_dense_rank_credits).alias("credits_dense_rank_credits_min_credits"),
    )
)

without_matches = (
    windowed.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.id")) & (F.col("e.term").isin("2024T1", "2024T2", "2025T1")) & (F.col("e.grade_status").isin("FAIL", "PASS")), "left_anti")
)

result = (
    without_matches
    .cube("credits", "seq_no")
    .agg(
        F.count("*").alias("n_rows"),
        F.percentile_approx("credits_cume_dist", 0.5).alias("median_credits_cume_dist"),
    )
)
