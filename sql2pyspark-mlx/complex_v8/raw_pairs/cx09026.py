from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id_prior_rows_3d = Window.partitionBy("student_id", "prior_rows_3d")
w_all_by_enrolled_at_running = Window.orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

joined_ranges = (
    df_enrollments.alias("a")
    .join(
        df_enrollments.alias("b"),
        (F.col("b.student_id") == F.col("a.student_id"))
        & (F.col("b.enrolled_at") < F.col("a.enrolled_at"))
        & (F.col("b.enrolled_at") >= F.col("a.enrolled_at") - F.expr("INTERVAL 3 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.student_id", "a.enrolled_at")
    .agg(
        F.count("b.id").alias("prior_rows_3d"),
        F.sum("b.score").alias("prior_score_3d"),
    )
)

with_trends = (
    joined_ranges
    .select(
        "*",
        F.count(F.lit(1)).over(w_student_id_prior_rows_3d).alias("rows_in_student_id_prior_rows_3d"),
        F.round(F.avg("prior_score_3d").over(w_student_id_prior_rows_3d), 2).alias("student_id_prior_rows_3d_avg_prior_score_3d"),
        F.count(F.lit(1)).over(w_all_by_enrolled_at_running).alias("running_count"),
    )
)

grouped = (
    with_trends
    .groupBy(F.year("enrolled_at").alias("enrolled_at_year"))
    .agg(
        F.max("enrolled_at").alias("last_enrolled_at"),
        F.percentile_approx("student_id_prior_rows_3d_avg_prior_score_3d", 0.5).alias("median_student_id_prior_rows_3d_avg_prior_score_"),
        F.round(F.sum("running_count") / F.nullif(F.sum("prior_rows_3d"), F.lit(0)), 3).alias("running_count_to_prior_rows_3d_ratio"),
        F.countDistinct("id").alias("distinct_ids"),
        F.max("rows_in_student_id_prior_rows_3d").alias("max_rows_in_student_id_prior_rows_3d"),
        F.max("student_id_prior_rows_3d_avg_prior_score_3d").alias("max_student_id_prior_rows_3d_avg_prior_score_3d_"),
    )
    .filter(F.col("max_student_id_prior_rows_3d_avg_prior_score_3d_") > 75.0)
    .drop("max_student_id_prior_rows_3d_avg_prior_score_3d_")
)

annotated = (
    grouped
    .select(
        "enrolled_at_year",
        "last_enrolled_at",
        "median_student_id_prior_rows_3d_avg_prior_score_",
        "running_count_to_prior_rows_3d_ratio",
        "distinct_ids",
        "max_rows_in_student_id_prior_rows_3d",
        F.date_format(F.col("last_enrolled_at"), "yyyy-MM").alias("last_enrolled_at_label"),
        (F.col("enrolled_at_year") + F.col("distinct_ids")).alias("enrolled_at_year_plus_distinct_ids"),
    )
)

result = (
    annotated
    .filter(F.col("max_rows_in_student_id_prior_rows_3d") >= 7)
    .select(
        "enrolled_at_year",
        "last_enrolled_at",
        "max_rows_in_student_id_prior_rows_3d",
        "last_enrolled_at_label",
        "enrolled_at_year_plus_distinct_ids",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("last_enrolled_at")).alias("days_since_last_enrolled_at"),
        F.lower(F.regexp_replace(F.trim(F.col("last_enrolled_at_label")), " ", "_")).alias("last_enrolled_at_label_slug"),
    )
    .orderBy(
        F.col("last_enrolled_at").desc(),
        F.col("enrolled_at_year_plus_distinct_ids").desc_nulls_first(),
    )
)
