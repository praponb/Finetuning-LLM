from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_grade_status_len_ties = Window.orderBy("grade_status_len")
w_all_by_score_filled_full = Window.orderBy("score_filled", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_grade_status_len_grade_statu = Window.partitionBy("grade_status_len", "grade_status").orderBy(F.col("enrolled_at").desc(), "id")
w_all_by_score_filled_ties = Window.orderBy("score_filled")

valid_enrollments = (
    df_enrollments
    .filter(~F.col("term").isin("2024T2", "2025T2"))
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
    )
)

with_trends = (
    valid_enrollments
    .select(
        "id",
        "score",
        "grade_status",
        "enrolled_at",
        "grade_status_len",
        "score_filled",
        F.round(F.percent_rank().over(w_all_by_grade_status_len_ties), 3).alias("grade_status_len_percent_rank"),
        F.nth_value("enrolled_at", 2).over(w_all_by_score_filled_full).alias("second_enrolled_at"),
        (F.col("score") - F.lag("score").over(w_grade_status_len_grade_statu)).alias("score_change"),
        F.round(F.cume_dist().over(w_grade_status_len_grade_statu), 3).alias("enrolled_at_cume_dist"),
        F.rank().over(w_all_by_score_filled_ties).alias("score_filled_rank"),
        F.round(F.percent_rank().over(w_grade_status_len_grade_statu), 3).alias("enrolled_at_percent_rank"),
    )
)

grade_status_stats = (
    with_trends
    .groupBy("grade_status")
    .agg(F.avg("score_filled").alias("avg_score_filled"))
)

vs_group = (
    with_trends.alias("s")
    .join(grade_status_stats.alias("g"), F.col("g.grade_status") == F.col("s.grade_status"), "inner")
    .filter(F.col("s.score_filled") > F.col("g.avg_score_filled"))
    .select(
        "s.*",
        "g.avg_score_filled",
        F.round(F.col("s.score_filled") - F.col("g.avg_score_filled"), 2).alias("score_filled_minus_avg"),
    )
)

result = (
    vs_group
    .select(
        "id",
        "avg_score_filled",
        "score_filled",
        "score",
    )
    .unpivot(
        ids=["id"],
        values=["avg_score_filled", "score_filled", "score"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("id", "measure")
)
