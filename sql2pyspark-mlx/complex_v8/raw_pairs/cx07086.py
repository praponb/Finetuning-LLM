from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_distinct_ids = Window.partitionBy("distinct_ids")
w_last_enrolled_at_year_by_n_r = Window.partitionBy(F.year("last_enrolled_at")).orderBy("n_rows", "enrolled_at_month", "term").rowsBetween(-1, Window.currentRow)
w_last_enrolled_at_year_by_n_r_2 = Window.partitionBy(F.year("last_enrolled_at")).orderBy("n_rows", "enrolled_at_month", "term").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_last_enrolled_at_year_by_n_r_3 = Window.partitionBy(F.year("last_enrolled_at")).orderBy("n_rows", "enrolled_at_month", "term")
w_last_enrolled_at_year_by_n_r_4 = Window.partitionBy(F.year("last_enrolled_at")).orderBy("n_rows")
w_first_enrolled_at_date_month = Window.partitionBy(F.date_trunc("month", "first_enrolled_at_date"), F.month("enrolled_at_month")).orderBy(F.col("n_rows_vs_distinct_ids_avg").desc(), "enrolled_at_month", "term").rowsBetween(-6, Window.currentRow)
w_first_enrolled_at_date_month_2 = Window.partitionBy(F.date_trunc("month", "first_enrolled_at_date"), F.month("enrolled_at_month")).orderBy(F.col("n_rows_vs_distinct_ids_avg").desc())
w_first_enrolled_at_date_month_3 = Window.partitionBy(F.date_trunc("month", "first_enrolled_at_date"), F.month("enrolled_at_month")).orderBy(F.col("n_rows_vs_distinct_ids_avg").desc(), "enrolled_at_month", "term")
w_all_by_min_n_rows_so_far = Window.orderBy("min_n_rows_so_far", "enrolled_at_month", "term")

recent = (
    df_enrollments
    .filter((F.col("course_id").isNotNull()) & (F.col("student_id") >= 9))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
        F.lower(F.trim(F.col("grade_status"))).alias("grade_status_clean"),
        F.substring(F.trim(F.col("term")), 1, 4).alias("term_prefix"),
    )
)

screened = (
    recent.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.grade_status") == "FAIL"), "left_anti")
)

grouped = (
    screened
    .groupBy(F.date_trunc("month", "enrolled_at").alias("enrolled_at_month"), "term")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.min("enrolled_at").alias("first_enrolled_at"),
        F.count("*").alias("n_rows"),
        F.array_sort(F.collect_set("grade_status")).alias("grade_status_list"),
        F.max("enrolled_at").alias("last_enrolled_at"),
    )
)

with_windows = (
    grouped
    .select(
        "enrolled_at_month",
        "term",
        "first_enrolled_at",
        "n_rows",
        "last_enrolled_at",
        F.round(F.col("n_rows") - F.avg("n_rows").over(w_distinct_ids), 2).alias("n_rows_vs_distinct_ids_avg"),
        F.max("distinct_ids").over(w_last_enrolled_at_year_by_n_r).alias("distinct_ids_max_2"),
        F.min("n_rows").over(w_last_enrolled_at_year_by_n_r_2).alias("min_n_rows_so_far"),
        F.ntile(10).over(w_last_enrolled_at_year_by_n_r_3).alias("n_rows_decile"),
        F.rank().over(w_last_enrolled_at_year_by_n_r_4).alias("n_rows_rank"),
        F.lag("last_enrolled_at", 3).over(w_last_enrolled_at_year_by_n_r_3).alias("prev3_last_enrolled_at"),
    )
)

scored = (
    with_windows
    .select(
        "enrolled_at_month",
        "term",
        "first_enrolled_at",
        "last_enrolled_at",
        "n_rows_vs_distinct_ids_avg",
        "distinct_ids_max_2",
        "min_n_rows_so_far",
        "n_rows_rank",
        "prev3_last_enrolled_at",
        F.lower(F.regexp_replace(F.trim(F.col("term")), " ", "_")).alias("term_slug"),
        F.concat(F.col("term"), F.lit("#"), F.col("distinct_ids_max_2").cast("string")).alias("term_ref"),
        F.to_date(F.col("first_enrolled_at")).alias("first_enrolled_at_date"),
    )
)

filtered = (
    scored
    .crossJoin(scored.agg(F.avg("n_rows_rank").alias("overall_avg_n_rows_rank")))
    .filter(F.col("n_rows_rank") > F.col("overall_avg_n_rows_rank"))
    .drop("overall_avg_n_rows_rank")
)

metrics = (
    filtered
    .select(
        "*",
        F.max("n_rows_vs_distinct_ids_avg").over(w_first_enrolled_at_date_month).alias("n_rows_vs_distinct_ids_avg_max_7"),
        F.round(F.cume_dist().over(w_first_enrolled_at_date_month_2), 3).alias("n_rows_vs_distinct_ids_avg_cume_dist"),
        F.round((F.unix_timestamp("first_enrolled_at") - F.unix_timestamp(F.lag("first_enrolled_at").over(w_first_enrolled_at_date_month_3))) / 60, 1).alias("minutes_since_prev_first_enrolled_at"),
        F.ntile(10).over(w_all_by_min_n_rows_so_far).alias("min_n_rows_so_far_decile"),
    )
)

result = (
    metrics
    .filter(F.col("term_slug").isNotNull())
    .select(
        "enrolled_at_month",
        "term",
        "last_enrolled_at",
        "n_rows_vs_distinct_ids_avg",
        "distinct_ids_max_2",
        "min_n_rows_so_far",
        "prev3_last_enrolled_at",
        "term_slug",
        "term_ref",
        "n_rows_vs_distinct_ids_avg_cume_dist",
        "min_n_rows_so_far_decile",
    )
    .orderBy(F.col("term_slug").asc_nulls_last())
)
