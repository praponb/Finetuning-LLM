from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_enrolled_at_year_prior_rows = Window.partitionBy(F.year("enrolled_at"), "prior_rows_12h").orderBy(F.col("prior_rows_12h").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_enrolled_at_year_prior_rows_2 = Window.partitionBy(F.year("enrolled_at"), "prior_rows_12h").orderBy(F.col("prior_rows_12h").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_prior_rows_12h_by_prior_rows = Window.partitionBy("prior_rows_12h").orderBy(F.col("prior_rows_12h").desc(), "id").rowsBetween(-6, Window.currentRow)
w_enrolled_at_year_prior_rows_3 = Window.partitionBy(F.year("enrolled_at"), "prior_rows_12h").orderBy(F.col("prior_rows_12h").desc(), "id")
w_enrolled_at_year_prior_rows_4 = Window.partitionBy(F.year("enrolled_at"), "prior_rows_12h").orderBy(F.col("prior_rows_12h").desc(), "id").rowsBetween(-6, Window.currentRow)
w_student_id_by_minutes_since = Window.partitionBy("student_id").orderBy(F.col("minutes_since_prev_enrolled_at").desc(), "id").rowsBetween(-3, -1)
w_next_enrolled_at_to_date_enr = Window.partitionBy(F.to_date("next_enrolled_at"), F.month("enrolled_at"))
w_enrollment_grade_status_coun = Window.partitionBy("enrollment_grade_status_count", "prior_rows_12h").orderBy("minutes_since_prev_enrolled_at")
w_student_id_by_minutes_since_2 = Window.partitionBy("student_id").orderBy(F.col("minutes_since_prev_enrolled_at").desc(), "id")

covered = (
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

with_trends = (
    covered
    .select(
        "*",
        F.max("prior_rows_12h").over(w_enrolled_at_year_prior_rows).alias("max_prior_rows_12h_so_far"),
        F.first("prior_score_12h").over(w_enrolled_at_year_prior_rows_2).alias("first_prior_score_12h"),
        F.sum("prior_rows_12h").over(w_prior_rows_12h_by_prior_rows).alias("prior_rows_12h_sum_7"),
        F.lead("enrolled_at").over(w_enrolled_at_year_prior_rows_3).alias("next_enrolled_at"),
        F.round(F.avg("prior_rows_12h").over(w_enrolled_at_year_prior_rows_4), 2).alias("prior_rows_12h_avg_7"),
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_enrolled_at_year_prior_rows_3))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.count(F.when(~F.col("grade_status").isin("PASS", "INCOMPLETE"), 1)).alias("enrollment_grade_status_count"),
        F.max("score").alias("max_enrollment_score"),
        F.count("*").alias("enrollment_count"),
    )
)

summarized_rows = (
    with_trends.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_grade_status_count"), F.lit(0)).alias("enrollment_grade_status_count"),
        "e.max_enrollment_score",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

ranked = (
    summarized_rows
    .select(
        "*",
        F.round(F.avg("prior_rows_12h_sum_7").over(w_student_id_by_minutes_since), 2).alias("prior_rows_12h_sum_7_avg_3_prior"),
        F.round(F.col("prior_rows_12h_avg_7") - F.avg("prior_rows_12h_avg_7").over(w_next_enrolled_at_to_date_enr), 2).alias("prior_rows_12h_avg_7_vs_next_enrolled_at_to_date"),
        F.round(F.col("prior_rows_12h") / F.nullif(F.sum("prior_rows_12h").over(w_next_enrolled_at_to_date_enr), F.lit(0)), 4).alias("prior_rows_12h_share_of_next_enrolled_at_to_date"),
        F.round(F.cume_dist().over(w_enrollment_grade_status_coun), 3).alias("minutes_since_prev_enrolled_at_cume_dist"),
        F.lag("prior_rows_12h").over(w_student_id_by_minutes_since_2).alias("prev_prior_rows_12h"),
        F.sum("prior_rows_12h").over(w_next_enrolled_at_to_date_enr).alias("next_enrolled_at_to_date_enrolled_at_month_sum_p"),
    )
)

result = (
    ranked
    .cube("enrollment_count", "enrollment_grade_status_count")
    .agg(
        F.round(F.avg("first_prior_score_12h"), 2).alias("avg_first_prior_score_12h"),
        F.max("prior_rows_12h_avg_7").alias("max_prior_rows_12h_avg_7"),
    )
    .orderBy(
        F.col("enrollment_count").asc_nulls_last(),
        F.col("enrollment_grade_status_count").asc_nulls_last(),
    )
)
