from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_row_count = Window.orderBy(F.col("row_count").desc_nulls_first(), "prior_rows_7d", "student_id")
w_row_count_prior_rows_7d = Window.partitionBy("row_count", "prior_rows_7d")

in_window = (
    df_enrollments.alias("a")
    .join(
        df_enrollments.alias("b"),
        (F.col("b.student_id") == F.col("a.student_id"))
        & (F.col("b.enrolled_at") < F.col("a.enrolled_at"))
        & (F.col("b.enrolled_at") >= F.col("a.enrolled_at") - F.expr("INTERVAL 7 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.student_id", "a.enrolled_at")
    .agg(
        F.count("b.id").alias("prior_rows_7d"),
        F.sum("b.score").alias("prior_score_7d"),
    )
)

scored = (
    in_window
    .filter(F.col("prior_score_7d") >= 125.0)
    .select(
        "*",
        F.date_trunc("quarter", F.col("enrolled_at")).alias("enrolled_at_quarter"),
    )
)

rollup_base = (
    scored
    .filter(F.month(F.col("enrolled_at_quarter")).isin(2, 3))
    .groupBy("prior_rows_7d", "student_id")
    .agg(
        F.round(F.sum("prior_rows_7d") / F.nullif(F.sum("prior_score_7d"), F.lit(0)), 3).alias("prior_rows_7d_to_prior_score_7d_ratio"),
        F.count("*").alias("row_count"),
    )
)

with_windows = (
    rollup_base
    .select(
        "*",
        F.ntile(3).over(w_all_by_row_count).alias("row_count_tercile"),
        F.round(F.col("prior_rows_7d") - F.avg("prior_rows_7d").over(w_row_count_prior_rows_7d), 2).alias("prior_rows_7d_vs_row_count_prior_rows_7d_avg"),
    )
)

result = (
    with_windows
    .filter(F.col("row_count_tercile") > 4)
    .select(
        "prior_rows_7d",
        "student_id",
        "prior_rows_7d_to_prior_score_7d_ratio",
        "row_count",
        "prior_rows_7d_vs_row_count_prior_rows_7d_avg",
        F.round(F.col("row_count") / F.nullif(F.col("row_count_tercile"), F.lit(0)), 3).alias("row_count_per_row_count_tercile"),
        F.round(F.col("row_count_tercile") / F.nullif(F.col("row_count"), F.lit(0)), 3).alias("row_count_tercile_per_row_count"),
    )
    .orderBy(
        F.col("prior_rows_7d_vs_row_count_prior_rows_7d_avg").asc(),
        F.col("row_count").desc(),
        "prior_rows_7d",
        "student_id",
    )
    .limit(5)
)
