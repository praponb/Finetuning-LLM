from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_enrolled_at = Window.orderBy(F.col("enrolled_at").desc_nulls_first(), "id")
w_enrolled_at_month_month_by_e = Window.partitionBy(F.month("enrolled_at_month")).orderBy(F.col("enrolled_at").desc())
w_course_id = Window.partitionBy("course_id")
w_all_by_enrolled_at_month_pri = Window.orderBy(F.col("enrolled_at_month").desc(), "id").rowsBetween(-6, -1)
w_all_by_enrolled_at_running = Window.orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_enrolled_at_month_year_row_c = Window.partitionBy("enrolled_at_month_year", "row_count").orderBy("stddev_score_vs_course_id_avg", "enrolled_at_month_year")
w_all_by_stddev_score_vs_cours = Window.orderBy("stddev_score_vs_course_id_avg", "enrolled_at_month_year")
w_row_count = Window.partitionBy("row_count")

prepared = (
    df_enrollments
    .filter((F.col("grade_status").isNotNull()) & (F.col("course_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.date_trunc("month", F.col("enrolled_at")).alias("enrolled_at_month"),
        F.concat_ws(" / ", F.col("term"), F.col("grade_status")).alias("term_grade_status_label"),
    )
)

metrics = (
    prepared
    .select(
        "*",
        F.rank().over(w_all_by_enrolled_at).alias("enrolled_at_rank"),
        F.dense_rank().over(w_enrolled_at_month_month_by_e).alias("enrolled_at_dense_rank"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_course_id), F.lit(0)), 4).alias("score_share_of_course_id"),
        F.min("score").over(w_course_id).alias("course_id_min_score"),
        F.round(F.col("score") - F.avg("score").over(w_course_id), 2).alias("score_vs_course_id_avg"),
        F.sum("score").over(w_all_by_enrolled_at_month_pri).alias("score_sum_6_prior"),
        F.count(F.lit(1)).over(w_all_by_enrolled_at_running).alias("running_count"),
        F.round(F.cume_dist().over(w_enrolled_at_month_month_by_e), 3).alias("enrolled_at_cume_dist"),
    )
)

derived = (
    metrics
    .filter(F.col("course_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        "enrolled_at_month",
        "enrolled_at_rank",
        "enrolled_at_dense_rank",
        "score_share_of_course_id",
        "score_vs_course_id_avg",
        "score_sum_6_prior",
        F.when(F.col("grade_status").isin("INCOMPLETE", "FAIL"), "group_a").when(F.col("grade_status") == "PASS", "group_b").otherwise("other").alias("grade_status_group"),
        F.length(F.trim(F.col("term"))).alias("term_len"),
    )
)

grouped = (
    derived
    .groupBy(F.year("enrolled_at_month").alias("enrolled_at_month_year"))
    .agg(
        F.array_sort(F.collect_set("term")).alias("term_list"),
        F.count("*").alias("row_count"),
        F.sum("score_sum_6_prior").alias("total_score_sum_6_prior"),
        F.round(F.stddev("score_vs_course_id_avg"), 2).alias("stddev_score_vs_course_id_avg"),
    )
)

with_windows = (
    grouped
    .select(
        "*",
        F.ntile(4).over(w_enrolled_at_month_year_row_c).alias("stddev_score_vs_course_id_avg_quartile"),
        F.lag("total_score_sum_6_prior").over(w_all_by_stddev_score_vs_cours).alias("prev_total_score_sum_6_prior"),
        F.min("row_count").over(w_row_count).alias("row_count_min_row_count"),
    )
)

result = (
    with_windows
    .select(
        "enrolled_at_month_year",
        "term_list",
        "row_count",
        "total_score_sum_6_prior",
        "stddev_score_vs_course_id_avg",
        "stddev_score_vs_course_id_avg_quartile",
    )
    .orderBy(
        F.col("total_score_sum_6_prior").desc(),
        "row_count",
        "enrolled_at_month_year",
    )
    .limit(10)
)
