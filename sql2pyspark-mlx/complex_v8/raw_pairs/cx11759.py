from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_prior_rows_12h_student_id = Window.partitionBy("prior_rows_12h", "student_id")
w_all_by_enrolled_at = Window.orderBy("enrolled_at", "id")
w_all_by_enrolled_at_ties = Window.orderBy("enrolled_at")

in_window = (
    df_enrollments.alias("a")
    .join(
        df_enrollments.alias("b"),
        (F.col("b.student_id") == F.col("a.student_id"))
        & (F.col("b.enrolled_at") < F.col("a.enrolled_at"))
        & (F.col("b.enrolled_at") >= F.col("a.enrolled_at") - F.expr("INTERVAL 12 HOURS")),
        "left",
    )
    .groupBy("a.id", "a.student_id", "a.enrolled_at")
    .agg(
        F.count("b.id").alias("prior_rows_12h"),
        F.sum("b.score").alias("prior_score_12h"),
    )
)

metrics = (
    in_window
    .select(
        "*",
        F.round(F.col("prior_rows_12h") / F.nullif(F.sum("prior_rows_12h").over(w_prior_rows_12h_student_id), F.lit(0)), 4).alias("prior_rows_12h_share_of_prior_rows_12h_student_i"),
        (F.col("prior_rows_12h") - F.lag("prior_rows_12h").over(w_all_by_enrolled_at)).alias("prior_rows_12h_change"),
        F.round(F.cume_dist().over(w_all_by_enrolled_at_ties), 3).alias("enrolled_at_cume_dist"),
    )
)

student_id_stats = (
    metrics
    .groupBy("student_id")
    .agg(F.avg("prior_rows_12h_change").alias("avg_prior_rows_12h_change"))
)

compared = (
    metrics.alias("s")
    .join(student_id_stats.alias("g"), F.col("g.student_id") == F.col("s.student_id"), "inner")
    .filter(F.col("s.prior_rows_12h_change") > F.col("g.avg_prior_rows_12h_change"))
    .select(
        "s.*",
        "g.avg_prior_rows_12h_change",
        F.round(F.col("s.prior_rows_12h_change") - F.col("g.avg_prior_rows_12h_change"), 2).alias("prior_rows_12h_change_minus_avg"),
    )
)

result = (
    compared
    .filter(F.col("prior_rows_12h").between(1, 7))
    .select(
        "id",
        "student_id",
        "enrolled_at",
        "prior_rows_12h",
        "prior_score_12h",
        "prior_rows_12h_share_of_prior_rows_12h_student_i",
        "prior_rows_12h_change",
        "prior_rows_12h_change_minus_avg",
        (F.col("prior_rows_12h_change") + F.col("prior_score_12h")).alias("prior_rows_12h_change_plus_prior_score_12h"),
    )
    .orderBy(F.col("id").desc())
)
