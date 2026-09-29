from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_prior_rows_12h_student_id = Window.partitionBy("prior_rows_12h", "student_id")
w_student_id_by_enrolled_at_pr = Window.partitionBy("student_id").orderBy("enrolled_at", "id").rowsBetween(-1, -1)

matched = (
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
    matched
    .select(
        "*",
        F.round(F.col("prior_score_12h") / F.nullif(F.sum("prior_score_12h").over(w_prior_rows_12h_student_id), F.lit(0)), 4).alias("prior_score_12h_share_of_prior_rows_12h_student_"),
        F.count(F.lit(1)).over(w_prior_rows_12h_student_id).alias("rows_in_prior_rows_12h_student_id"),
        F.round(F.avg("prior_rows_12h").over(w_student_id_by_enrolled_at_pr), 2).alias("prior_rows_12h_avg_1_prior"),
    )
)

without_matches = (
    metrics.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.grade_status") != "WITHDRAWN") & (F.col("e.score") <= 81.4), "left_anti")
)

result = (
    without_matches
    .select(
        "id",
        "student_id",
        "enrolled_at",
        "prior_rows_12h",
        "prior_score_12h",
        "prior_rows_12h_avg_1_prior",
        F.when(F.col("prior_rows_12h") >= 6, "A").when(F.col("prior_rows_12h") >= 3, "B").otherwise("C").alias("prior_rows_12h_tier"),
    )
    .orderBy(F.col("student_id").desc())
)
