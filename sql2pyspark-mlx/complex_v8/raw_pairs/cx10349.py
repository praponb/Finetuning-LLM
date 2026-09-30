from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_prior_rows_12h_by_prior_scor = Window.partitionBy("prior_rows_12h").orderBy("prior_score_12h", "id")
w_all_by_enrolled_at_prior_2 = Window.orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(-2, -1)
w_all_by_enrolled_at = Window.orderBy(F.col("enrolled_at").desc(), "id")
w_prior_rows_12h_student_id = Window.partitionBy("prior_rows_12h", "student_id")

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

ranked = (
    in_window
    .select(
        "id",
        "prior_rows_12h",
        "prior_score_12h",
        F.lag("prior_rows_12h").over(w_prior_rows_12h_by_prior_scor).alias("prev_prior_rows_12h"),
        F.sum("prior_score_12h").over(w_all_by_enrolled_at_prior_2).alias("prior_score_12h_sum_2_prior"),
        F.lead("prior_rows_12h").over(w_prior_rows_12h_by_prior_scor).alias("next_prior_rows_12h"),
        (F.col("prior_score_12h") - F.lag("prior_score_12h").over(w_all_by_enrolled_at)).alias("prior_score_12h_change"),
        F.round(F.col("prior_rows_12h") - F.avg("prior_rows_12h").over(w_prior_rows_12h_student_id), 2).alias("prior_rows_12h_vs_prior_rows_12h_student_id_avg"),
        F.round(F.col("prior_rows_12h") / F.nullif(F.sum("prior_rows_12h").over(w_prior_rows_12h_student_id), F.lit(0)), 4).alias("prior_rows_12h_share_of_prior_rows_12h_student_i"),
    )
)

screened = (
    ranked
    .crossJoin(ranked.agg(F.min("prev_prior_rows_12h").alias("overall_min_prev_prior_rows_12h")))
    .filter(F.col("prev_prior_rows_12h") > F.col("overall_min_prev_prior_rows_12h"))
    .drop("overall_min_prev_prior_rows_12h")
)

result = (
    screened
    .cube("next_prior_rows_12h", "prev_prior_rows_12h", "prior_rows_12h")
    .agg(F.countDistinct("id").alias("distinct_ids"))
    .orderBy(
        F.col("next_prior_rows_12h").asc_nulls_last(),
        F.col("prev_prior_rows_12h").asc_nulls_last(),
        F.col("prior_rows_12h").asc_nulls_last(),
    )
)
