from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term_by_n_term_in_2025t1_202 = Window.partitionBy("term").orderBy(F.col("n_term_in_2025t1_2024t1_distance_2").desc(), "enrolled_at_month", "grade_status", "term").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_term_by_n_term_in_2025t1_202_2 = Window.partitionBy("term").orderBy(F.col("n_term_in_2025t1_2024t1_distance_2").desc(), "enrolled_at_month", "grade_status", "term")
w_n_term_in_2025t1_2024t1 = Window.partitionBy("n_term_in_2025t1_2024t1")
w_term_by_n_term_in_2025t1_202_3 = Window.partitionBy("term").orderBy(F.col("n_term_in_2025t1_2024t1_distance_2").desc(), "enrolled_at_month", "grade_status", "term").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped_enrollments = (
    df_enrollments
    .filter((F.col("student_id") <= 11) & (F.col("grade_status").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.initcap(F.trim(F.col("grade_status"))).alias("grade_status_clean"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
        F.when(F.col("score") >= 62.5, "A").when(F.col("score") >= 34.2, "B").otherwise("C").alias("score_tier"),
    )
)

summary = (
    scoped_enrollments
    .groupBy(
        F.date_trunc("month", "enrolled_at").alias("enrolled_at_month"),
        "grade_status",
        "term",
    )
    .agg(
        F.sum("days_since_enrolled_at").alias("total_days_since_enrolled_at"),
        F.sum(F.when(F.col("term").isin("2025T1", "2024T1"), 1).otherwise(0)).alias("n_term_in_2025t1_2024t1"),
        F.array_sort(F.collect_set("grade_status")).alias("grade_status_list"),
        F.round(100 * F.count(F.when(~F.col("grade_status").isin("INCOMPLETE", "PASS"), 1)) / F.count("*"), 1).alias("pct_grade_status_not_in_incomple"),
    )
)

enriched = (
    summary
    .filter(F.col("n_term_in_2025t1_2024t1") < 7)
    .select(
        "*",
        F.date_trunc("year", F.col("enrolled_at_month")).alias("enrolled_at_month_year"),
        F.abs(F.col("n_term_in_2025t1_2024t1") - 5).alias("n_term_in_2025t1_2024t1_distance"),
        F.abs(F.col("n_term_in_2025t1_2024t1") - 6).alias("n_term_in_2025t1_2024t1_distance_2"),
    )
)

with_trends = (
    enriched
    .select(
        "*",
        F.first("enrolled_at_month", ignorenulls=True).over(w_term_by_n_term_in_2025t1_202).alias("first_known_enrolled_at_month"),
        F.lead("total_days_since_enrolled_at").over(w_term_by_n_term_in_2025t1_202_2).alias("next_total_days_since_enrolled_at"),
        F.count(F.lit(1)).over(w_n_term_in_2025t1_2024t1).alias("rows_in_n_term_in_2025t1_2024t1"),
        F.nth_value("enrolled_at_month", 2).over(w_term_by_n_term_in_2025t1_202_3).alias("second_enrolled_at_month"),
        F.sum("n_term_in_2025t1_2024t1").over(w_n_term_in_2025t1_2024t1).alias("n_term_in_2025t1_2024t1_sum_n_term_in_2025t1_202"),
    )
)

result = (
    with_trends
    .groupingSets(
        [["n_term_in_2025t1_2024t1", "term"], ["n_term_in_2025t1_2024t1"], []],
        "n_term_in_2025t1_2024t1", "term",
    )
    .agg(
        F.grouping("n_term_in_2025t1_2024t1").alias("is_total_n_term_in_2025t1_2024t1"),
        F.round(F.avg("n_term_in_2025t1_2024t1_distance_2"), 2).alias("avg_n_term_in_2025t1_2024t1_distance_2"),
        F.round(100 * F.count(F.when(F.col("term").isNotNull(), 1)) / F.count("*"), 1).alias("pct_term_is_not_null"),
        F.count(F.when(F.col("grade_status") != "WITHDRAWN", 1)).alias("n_grade_status_withdrawn"),
    )
    .orderBy(
        F.col("n_term_in_2025t1_2024t1").asc_nulls_last(),
        F.col("term").asc_nulls_last(),
    )
)
