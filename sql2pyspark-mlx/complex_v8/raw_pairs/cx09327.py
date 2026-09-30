from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("term").orderBy(F.col("score").desc(), F.col("id").desc())

recent = df_enrollments

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(F.sum("score").alias("total_enrollment_score"))
)

with_stats = (
    recent.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select("s.*", "e.total_enrollment_score")
)

latest = (
    with_stats
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

enrollment_stats_2 = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.sum("score").alias("total_enrollment_score_2"),
        F.count("*").alias("enrollment_count"),
    )
)

summarized_rows = (
    latest.alias("s")
    .join(enrollment_stats_2.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        "e.total_enrollment_score_2",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

stats = (
    summarized_rows
    .groupBy("enrollment_count")
    .agg(
        F.round(100 * F.count(F.when(F.col("grade_status") == "INCOMPLETE", 1)) / F.count("*"), 1).alias("pct_grade_status_incomplete"),
        F.array_sort(F.collect_set("term")).alias("term_list"),
        F.avg("total_enrollment_score").alias("avg_total_enrollment_score_check"),
    )
    .filter(F.col("avg_total_enrollment_score_check") > 180.0)
    .drop("avg_total_enrollment_score_check")
)

qualified = (
    stats
    .crossJoin(stats.agg(F.avg("pct_grade_status_incomplete").alias("overall_avg_pct_grade_status_incomplete")))
    .filter(F.col("pct_grade_status_incomplete") > F.col("overall_avg_pct_grade_status_incomplete"))
    .drop("overall_avg_pct_grade_status_incomplete")
)

flattened = (
    qualified
    .select(
        "enrollment_count",
        "pct_grade_status_incomplete",
        F.explode("term_list").alias("term_list_item"),
    )
)

result = (
    flattened
    .select(
        "enrollment_count",
        "pct_grade_status_incomplete",
        "term_list_item",
        F.lower(F.regexp_replace(F.trim(F.col("term_list_item")), " ", "_")).alias("term_list_item_slug"),
        F.length(F.trim(F.col("term_list_item"))).alias("term_list_item_len"),
    )
    .orderBy(F.col("term_list_item_slug").desc())
)
