from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_prior_rows_1d_by_prior_rows = Window.partitionBy("prior_rows_1d").orderBy(F.col("prior_rows_1d").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_prior_rows_1d_by_prior_rows_2 = Window.partitionBy("prior_rows_1d").orderBy(F.col("prior_rows_1d").asc_nulls_last(), "id")
w_enrolled_at_month_prior_rows = Window.partitionBy(F.month("enrolled_at"), "prior_rows_1d").orderBy("prior_score_1d", "id")
w_student_id = Window.partitionBy("student_id")

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

with_trends = (
    in_window
    .select(
        "id",
        "student_id",
        "enrolled_at",
        "prior_score_1d",
        F.first("enrolled_at").over(w_prior_rows_1d_by_prior_rows).alias("first_enrolled_at"),
        F.lag("prior_rows_1d").over(w_prior_rows_1d_by_prior_rows_2).alias("prev_prior_rows_1d"),
        (F.col("prior_rows_1d") - F.lag("prior_rows_1d").over(w_enrolled_at_month_prior_rows)).alias("prior_rows_1d_change"),
        F.sum("prior_score_1d").over(w_student_id).alias("student_id_sum_prior_score_1d"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "student_id",
        "enrolled_at",
        "prior_score_1d",
        "prev_prior_rows_1d",
        "student_id_sum_prior_score_1d",
        F.dayofweek(F.col("first_enrolled_at")).alias("first_enrolled_at_dayofweek"),
    )
)
