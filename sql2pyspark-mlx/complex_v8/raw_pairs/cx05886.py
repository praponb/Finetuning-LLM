from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_prior_score_7d = Window.orderBy(F.col("prior_score_7d").asc_nulls_last(), "id")
w_student_id = Window.partitionBy("student_id")

matched = (
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

with_trends = (
    matched
    .select(
        "*",
        F.row_number().over(w_all_by_prior_score_7d).alias("prior_score_7d_seq"),
        F.rank().over(w_all_by_prior_score_7d).alias("prior_score_7d_rank"),
        F.count(F.lit(1)).over(w_student_id).alias("rows_in_student_id"),
        F.round(F.col("prior_score_7d") / F.nullif(F.sum("prior_score_7d").over(w_student_id), F.lit(0)), 4).alias("prior_score_7d_share_of_student_id"),
        F.round(F.col("prior_rows_7d") - F.avg("prior_rows_7d").over(w_student_id), 2).alias("prior_rows_7d_vs_student_id_avg"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "student_id",
        "enrolled_at",
        "prior_rows_7d",
        "prior_score_7d",
        "prior_score_7d_seq",
        "prior_score_7d_share_of_student_id",
        "prior_rows_7d_vs_student_id_avg",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
    )
    .orderBy(
        F.col("prior_rows_7d_vs_student_id_avg").desc(),
        F.col("student_id").desc(),
    )
)
