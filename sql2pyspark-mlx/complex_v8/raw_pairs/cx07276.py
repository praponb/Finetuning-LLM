from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_prior_rows_7d_by_prior_score = Window.partitionBy("prior_rows_7d").orderBy(F.col("prior_score_7d").asc_nulls_last(), "id").rowsBetween(-1, -1)
w_student_id_by_enrolled_at = Window.partitionBy("student_id").orderBy("enrolled_at", "id")
w_student_id = Window.partitionBy("student_id")
w_prior_rows_7d_by_prior_rows = Window.partitionBy("prior_rows_7d").orderBy(F.col("prior_rows_7d").desc(), "id")

covered = (
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

windowed = (
    covered
    .select(
        "*",
        F.max("prior_rows_7d").over(w_prior_rows_7d_by_prior_score).alias("prior_rows_7d_max_1_prior"),
        F.lead("prior_score_7d", 1, 0).over(w_student_id_by_enrolled_at).alias("next_prior_score_7d"),
        F.round(F.col("prior_score_7d") / F.nullif(F.sum("prior_score_7d").over(w_student_id), F.lit(0)), 4).alias("prior_score_7d_share_of_student_id"),
        F.ntile(10).over(w_prior_rows_7d_by_prior_rows).alias("prior_rows_7d_decile"),
    )
)

result = (
    windowed
    .filter(F.col("prior_rows_7d_decile") >= 3)
    .select(
        "id",
        "student_id",
        "enrolled_at",
        "prior_rows_7d",
        "prior_score_7d",
        "prior_rows_7d_max_1_prior",
        "next_prior_score_7d",
        "prior_score_7d_share_of_student_id",
        "prior_rows_7d_decile",
    )
    .orderBy(
        F.col("prior_rows_7d_max_1_prior").desc(),
        F.col("prior_score_7d_share_of_student_id").desc(),
    )
)
