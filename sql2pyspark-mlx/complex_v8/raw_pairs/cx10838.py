from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dept_distinct_levels = Window.partitionBy("dept", "distinct_levels")
w_all_by_pct_level_in_intro_co = Window.orderBy("pct_level_in_intro_core", "level", "dept", "credits").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_pct_level_in_intro_co_2 = Window.orderBy("pct_level_in_intro_core", "level", "dept", "credits")
w_level_dept = Window.partitionBy("level", "dept")

prepared = (
    df_courses
    .filter(F.col("level") == "core")
    .select(
        "id",
        "dept",
        "credits",
        "level",
    )
)

grouped = (
    prepared
    .groupBy("level", "dept", "credits")
    .agg(
        F.round(100 * F.count(F.when(F.col("level").isin("intro", "core"), 1)) / F.count("*"), 1).alias("pct_level_in_intro_core"),
        F.countDistinct("level").alias("distinct_levels"),
        F.count("*").alias("n_rows"),
        F.sum("credits").alias("total_credits"),
        F.array_sort(F.collect_set("dept")).alias("dept_list"),
        F.max("credits").alias("max_credits_check"),
    )
    .filter(F.col("max_credits_check") > 2)
    .drop("max_credits_check")
)

with_activity = (
    grouped
    .crossJoin(grouped.agg(F.min("distinct_levels").alias("overall_min_distinct_levels")))
    .filter(F.col("distinct_levels") > F.col("overall_min_distinct_levels"))
    .drop("overall_min_distinct_levels")
)

with_trends = (
    with_activity
    .select(
        "level",
        "dept",
        "credits",
        "distinct_levels",
        "n_rows",
        "total_credits",
        "dept_list",
        F.count(F.lit(1)).over(w_dept_distinct_levels).alias("rows_in_dept_distinct_levels"),
        F.last("total_credits", ignorenulls=True).over(w_all_by_pct_level_in_intro_co).alias("last_known_total_credits"),
        F.ntile(4).over(w_all_by_pct_level_in_intro_co_2).alias("pct_level_in_intro_core_quartile"),
        F.row_number().over(w_all_by_pct_level_in_intro_co_2).alias("pct_level_in_intro_core_seq"),
        F.size(F.collect_set("level").over(w_dept_distinct_levels)).alias("distinct_level_in_dept_distinct_levels"),
    )
)

with_windows = (
    with_trends
    .select(
        "level",
        "dept",
        "credits",
        "distinct_levels",
        "n_rows",
        "rows_in_dept_distinct_levels",
        "last_known_total_credits",
        "pct_level_in_intro_core_quartile",
        "pct_level_in_intro_core_seq",
        "distinct_level_in_dept_distinct_levels",
        F.sum("pct_level_in_intro_core_seq").over(w_level_dept).alias("level_dept_sum_pct_level_in_intro_core_seq"),
        F.sum("last_known_total_credits").over(w_level_dept).alias("level_dept_sum_last_known_total_credits"),
    )
)

result = (
    with_windows
    .filter(F.col("distinct_levels") < 6)
    .select(
        "level",
        "dept",
        "credits",
        "distinct_levels",
        "n_rows",
        "rows_in_dept_distinct_levels",
        "last_known_total_credits",
        "pct_level_in_intro_core_quartile",
        "distinct_level_in_dept_distinct_levels",
        "level_dept_sum_last_known_total_credits",
        (F.col("n_rows") + F.col("last_known_total_credits")).alias("n_rows_plus_last_known_total_credits"),
        F.coalesce(F.col("last_known_total_credits"), F.lit(0)).alias("last_known_total_credits_filled"),
    )
    .orderBy(
        F.col("distinct_levels").desc_nulls_first(),
        F.col("last_known_total_credits").asc(),
    )
)
