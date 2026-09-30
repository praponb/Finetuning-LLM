from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("grade_status", "student_id").orderBy(F.col("score_distance").desc(), F.col("id").desc())
w_grade_status_n_grade_status = Window.partitionBy("grade_status", "n_grade_status_pass").orderBy(F.col("last_enrolled_at").desc_nulls_first(), "grade_status").rowsBetween(-6, Window.currentRow)
w_grade_status_n_grade_status_2 = Window.partitionBy("grade_status", "n_grade_status_pass").orderBy(F.col("min_days_since_enrolled_at").desc(), "grade_status").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_grade_status_n_grade_status_3 = Window.partitionBy("grade_status", "n_grade_status_pass").orderBy(F.col("last_enrolled_at").desc_nulls_first(), "grade_status").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_grade_status = Window.partitionBy("grade_status")

prepared_enrollments = (
    df_enrollments
    .filter(F.col("score") < 78.8)
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.abs(F.col("score") - 42.4).alias("score_distance"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
        F.concat_ws(" / ", F.col("grade_status"), F.col("term")).alias("grade_status_term_label"),
    )
)

with_activity = (
    prepared_enrollments.alias("s")
    .join(df_courses.alias("c"), (F.col("c.id") == F.col("s.course_id")) & (F.col("c.credits") <= 3), "left_anti")
)

qualified = (
    with_activity.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.student_id") <= 6), "left_anti")
)

latest = (
    qualified
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

grouped = (
    latest
    .filter(F.month(F.col("enrolled_at")).isin(1, 2))
    .groupBy("grade_status")
    .agg(
        F.min("score").alias("min_score"),
        F.min("days_since_enrolled_at").alias("min_days_since_enrolled_at"),
        F.max("enrolled_at").alias("last_enrolled_at"),
        F.round(100 * F.count(F.when(F.col("grade_status") == "INCOMPLETE", 1)) / F.count("*"), 1).alias("pct_grade_status_incomplete"),
        F.sum(F.when(F.col("grade_status") != "PASS", 1).otherwise(0)).alias("n_grade_status_pass"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

windowed = (
    grouped
    .select(
        "grade_status",
        "min_score",
        "last_enrolled_at",
        "pct_grade_status_incomplete",
        "n_grade_status_pass",
        "distinct_ids",
        F.round(F.avg("n_grade_status_pass").over(w_grade_status_n_grade_status), 2).alias("n_grade_status_pass_avg_7"),
        F.nth_value("n_grade_status_pass", 2).over(w_grade_status_n_grade_status_2).alias("second_n_grade_status_pass"),
        F.first("distinct_ids").over(w_grade_status_n_grade_status_3).alias("first_distinct_ids"),
        F.sum("distinct_ids").over(w_grade_status).alias("grade_status_sum_distinct_ids"),
    )
)

annotated = (
    windowed
    .select(
        "*",
        (F.col("first_distinct_ids") - F.col("n_grade_status_pass")).alias("first_distinct_ids_minus_n_grade_status_pass"),
        F.least(F.col("min_score"), F.col("n_grade_status_pass")).alias("least_min_score_n_grade_status_pass"),
        F.month(F.col("last_enrolled_at")).alias("last_enrolled_at_month"),
    )
)

result = (
    annotated
    .cube("grade_status", "last_enrolled_at_month", "first_distinct_ids")
    .agg(
        F.array_sort(F.collect_set("grade_status")).alias("grade_status_list"),
        F.round(F.sum("min_score"), 2).alias("total_min_score"),
        F.count("*").alias("cnt"),
    )
    .select(
        F.coalesce(F.col("grade_status"), F.lit("ALL")).alias("grade_status"),
        "last_enrolled_at_month",
        "first_distinct_ids",
        "grade_status_list",
        "total_min_score",
        "cnt",
    )
)
