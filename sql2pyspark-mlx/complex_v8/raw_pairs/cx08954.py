from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_prior_rows_1d = Window.partitionBy("prior_rows_1d")
w_all_by_prior_score_1d_moving = Window.orderBy("prior_score_1d", "id").rowsBetween(-6, Window.currentRow)

in_window = (
    df_enrollments.alias("a")
    .join(
        df_enrollments.alias("b"),
        (F.col("b.student_id") == F.col("a.student_id"))
        & (F.col("b.enrolled_at") < F.col("a.enrolled_at"))
        & (F.col("b.enrolled_at") >= F.col("a.enrolled_at") - F.expr("INTERVAL 1 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.student_id", "a.enrolled_at")
    .agg(
        F.count("b.id").alias("prior_rows_1d"),
        F.sum("b.score").alias("prior_score_1d"),
    )
)

with_windows = (
    in_window
    .select(
        "id",
        "student_id",
        "enrolled_at",
        "prior_score_1d",
        F.max("prior_rows_1d").over(w_prior_rows_1d).alias("prior_rows_1d_max_prior_rows_1d"),
        F.sum("prior_score_1d").over(w_all_by_prior_score_1d_moving).alias("prior_score_1d_sum_7"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "prior_score_1d",
        "prior_rows_1d_max_prior_rows_1d",
        "prior_score_1d_sum_7",
    )
    .orderBy("id", "prior_rows_1d_max_prior_rows_1d")
)
