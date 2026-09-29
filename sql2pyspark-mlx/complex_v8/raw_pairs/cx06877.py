from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_min_score = Window.orderBy(F.col("min_score").desc(), "course_id", "enrollment_id")
w_all_by_greatest_distinct_ids = Window.orderBy("greatest_distinct_ids_total_score", "course_id", "enrollment_id")
w_all_by_min_score_ties = Window.orderBy(F.col("min_score").desc())
w_all_by_greatest_distinct_ids_2 = Window.orderBy("greatest_distinct_ids_total_score", "course_id", "enrollment_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_course_id = Window.partitionBy("course_id")
w_distinct_ids_course_id_by_en = Window.partitionBy("distinct_ids", "course_id").orderBy("enrollment_enrolled_at", "course_id", "enrollment_id").rowsBetween(-1, Window.currentRow)
w_min_score_quintile_by_enroll = Window.partitionBy("min_score_quintile").orderBy(F.col("enrollment_enrolled_at").asc_nulls_last(), "course_id", "enrollment_id")

cleaned = (
    df_enrollments
    .filter(F.col("course_id") <= 3)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
    )
)

summary = (
    cleaned
    .filter(F.col("score") < 80.2)
    .groupBy("course_id")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.min("score").alias("min_score"),
        F.sum("score").alias("total_score"),
    )
)

scored = (
    summary
    .filter(F.col("total_score") > 150.0)
    .select(
        "*",
        F.abs(F.col("min_score") - 60.0).alias("min_score_distance"),
        F.greatest(F.col("distinct_ids"), F.col("total_score")).alias("greatest_distinct_ids_total_score"),
        F.round(F.col("distinct_ids") / F.nullif(F.col("total_score"), F.lit(0)), 3).alias("distinct_ids_per_total_score"),
    )
)

detail_rows = (
    scored.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.course_id",
        "s.distinct_ids",
        "s.min_score",
        "s.total_score",
        "s.greatest_distinct_ids_total_score",
        "s.distinct_ids_per_total_score",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.student_id").alias("enrollment_student_id"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
    )
)

ranked = (
    detail_rows
    .select(
        "course_id",
        "distinct_ids",
        "min_score",
        "total_score",
        "greatest_distinct_ids_total_score",
        "enrollment_id",
        "enrollment_student_id",
        "enrollment_enrolled_at",
        (F.col("distinct_ids") - F.lag("distinct_ids").over(w_all_by_min_score)).alias("distinct_ids_change"),
        F.dense_rank().over(w_all_by_greatest_distinct_ids).alias("greatest_distinct_ids_total_score_dense_rank"),
        F.dense_rank().over(w_all_by_min_score_ties).alias("min_score_dense_rank"),
        F.ntile(5).over(w_all_by_min_score).alias("min_score_quintile"),
        F.round(F.percent_rank().over(w_all_by_min_score_ties), 3).alias("min_score_percent_rank"),
        F.first("min_score", ignorenulls=True).over(w_all_by_greatest_distinct_ids_2).alias("first_known_min_score"),
    )
)

with_trends = (
    ranked
    .select(
        "course_id",
        "distinct_ids",
        "greatest_distinct_ids_total_score",
        "enrollment_id",
        "enrollment_enrolled_at",
        "distinct_ids_change",
        "min_score_dense_rank",
        "min_score_quintile",
        "min_score_percent_rank",
        F.round(F.col("total_score") - F.avg("total_score").over(w_course_id), 2).alias("total_score_vs_course_id_avg"),
        F.sum("min_score_quintile").over(w_distinct_ids_course_id_by_en).alias("min_score_quintile_sum_2"),
        (F.col("first_known_min_score") - F.lag("first_known_min_score").over(w_min_score_quintile_by_enroll)).alias("first_known_min_score_change"),
    )
)

derived = (
    with_trends
    .select(
        "*",
        F.date_trunc("quarter", F.col("enrollment_enrolled_at")).alias("enrollment_enrolled_at_quarter"),
        F.to_date(F.col("enrollment_enrolled_at")).alias("enrollment_enrolled_at_date"),
    )
)

result = (
    derived
    .select(
        "course_id",
        "distinct_ids",
        "greatest_distinct_ids_total_score",
        "enrollment_enrolled_at",
        "min_score_dense_rank",
        "min_score_quintile",
        "min_score_percent_rank",
        "total_score_vs_course_id_avg",
        "min_score_quintile_sum_2",
        "enrollment_enrolled_at_date",
        F.coalesce(F.col("min_score_dense_rank"), F.lit(0)).alias("min_score_dense_rank_filled"),
    )
    .orderBy(F.col("min_score_dense_rank").desc())
)
