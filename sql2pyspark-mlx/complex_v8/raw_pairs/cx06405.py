from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id_enrolled_at_to_da = Window.partitionBy("student_id", F.to_date("enrolled_at")).orderBy(F.col("prior_rows_1d").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_student_id_enrolled_at_to_da_2 = Window.partitionBy("student_id", F.to_date("enrolled_at")).orderBy(F.col("prior_rows_1d").desc(), "id").rowsBetween(-2, -1)
w_student_id_enrolled_at_to_da_3 = Window.partitionBy("student_id", F.to_date("enrolled_at")).orderBy(F.col("prior_rows_1d").desc(), "id")
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

sequenced = (
    in_window
    .select(
        "id",
        "prior_rows_1d",
        "prior_score_1d",
        F.nth_value("enrolled_at", 2).over(w_student_id_enrolled_at_to_da).alias("second_enrolled_at"),
        F.max("prior_score_1d").over(w_student_id_enrolled_at_to_da_2).alias("prior_score_1d_max_2_prior"),
        (F.col("prior_score_1d") - F.lag("prior_score_1d").over(w_student_id_enrolled_at_to_da_3)).alias("prior_score_1d_change"),
        F.round(F.col("prior_rows_1d") - F.avg("prior_rows_1d").over(w_student_id), 2).alias("prior_rows_1d_vs_student_id_avg"),
        F.sum("prior_score_1d").over(w_student_id).alias("student_id_sum_prior_score_1d"),
        F.lead("prior_score_1d", 3, 0).over(w_student_id_enrolled_at_to_da_3).alias("next3_prior_score_1d"),
    )
)

result = (
    sequenced
    .filter(F.col("prior_score_1d_max_2_prior") <= 1200.0)
    .select(
        "id",
        "prior_rows_1d",
        "prior_score_1d",
        "prior_score_1d_change",
        "prior_rows_1d_vs_student_id_avg",
    )
    .orderBy(F.col("prior_score_1d").asc_nulls_last(), "id")
    .limit(5)
)
