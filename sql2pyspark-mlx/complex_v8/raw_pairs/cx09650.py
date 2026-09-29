from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_prior_rows_1d_enrolled_at_ye = Window.partitionBy("prior_rows_1d", F.year("enrolled_at")).orderBy(F.col("prior_score_1d").desc(), "id")
w_all_by_enrolled_at = Window.orderBy("enrolled_at", "id")
w_all_by_enrolled_at_full = Window.orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

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
        "*",
        F.lead("prior_score_1d", 2, 0).over(w_prior_rows_1d_enrolled_at_ye).alias("next2_prior_score_1d"),
        F.lag("prior_score_1d", 1, 0).over(w_prior_rows_1d_enrolled_at_ye).alias("prev_prior_score_1d"),
        F.row_number().over(w_all_by_enrolled_at).alias("seq_no"),
        F.nth_value("prior_rows_1d", 2).over(w_all_by_enrolled_at_full).alias("second_prior_rows_1d"),
    )
)

stats = (
    with_trends
    .groupBy(
        F.when(F.col("prior_score_1d") >= 300.0, "A").when(F.col("prior_score_1d") >= 200.0, "B").otherwise("C").alias("prior_score_1d_tier"),
        F.date_format("enrolled_at", "yyyy-MM").alias("enrolled_at_ym"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("second_prior_rows_1d"), 2).alias("avg_second_prior_rows_1d"),
        F.percentile_approx("seq_no", 0.5).alias("median_seq_no"),
        F.count("*").alias("cnt"),
    )
)

result = (
    stats
    .groupBy("distinct_ids")
    .pivot("prior_score_1d_tier", ["C", "A"])
    .agg(F.sum("cnt"))
    .orderBy("distinct_ids")
)
