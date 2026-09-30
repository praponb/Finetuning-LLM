from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").desc_nulls_last(), "id")
w2 = Window.partitionBy("location").orderBy(F.col("rank_in_group").desc())
w3 = Window.partitionBy("dept_name")
w4 = Window.partitionBy("dept_name").orderBy("budget", "id")
w5 = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(-2, Window.currentRow)
w6 = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("dept_name").orderBy(F.col("dept_name_max_budget").desc_nulls_last())
w8 = Window.partitionBy("dept_name").orderBy(F.col("next_budget").desc(), "id")
w9 = Window.partitionBy("dept_name").orderBy("dept_name_max_budget_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("dept_name").orderBy("next_budget_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

kept = (
    df_departments
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.manager_id").isNotNull()), "left_anti")
)

latest = (
    kept
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

deduped = (
    latest
    .withColumn("row_num", F.rank().over(w2))
    .filter(F.col("row_num") == 1)
    .select("id", "dept_name", "budget")
)

with_windows = (
    deduped
    .filter(F.col("budget").isNull())
    .select(
        "*",
        F.max("budget").over(w3).alias("dept_name_max_budget"),
        F.lead("budget", 1).over(w4).alias("next_budget"),
        F.avg("budget").over(w5).alias("budget_avg_2"),
        F.last("dept_name", ignorenulls=True).over(w6).alias("last_known_dept_name"),
    )
)

scored = (
    with_windows
    .select(
        "id",
        "dept_name",
        "budget",
        "dept_name_max_budget",
        "next_budget",
        "budget_avg_2",
        F.rank().over(w7).alias("dept_name_max_budget_rank"),
        F.ntile(5).over(w8).alias("next_budget_bucket"),
        F.sum("budget").over(w6).alias("cumulative_budget"),
    )
)

with_windows_departments = (
    scored
    .withColumn("first_known_dept_name", F.first("dept_name", ignorenulls=True).over(w9))
    .withColumn("cumulative_dept_name_max_budget_rank", F.sum("dept_name_max_budget_rank").over(w_running))
)

cleaned = (
    with_windows_departments
    .filter(F.col("next_budget").isNull())
    .select(
        "id",
        "dept_name",
        "budget",
        "next_budget",
        "dept_name_max_budget_rank",
        "next_budget_bucket",
        "cumulative_budget",
        "first_known_dept_name",
        "cumulative_dept_name_max_budget_rank",
        F.greatest("next_budget", "cumulative_budget").alias("greatest_next_budget_cumulative_budget"),
        F.coalesce("dept_name", F.lit("UNKNOWN")).alias("dept_name_filled"),
    )
)

result = (
    cleaned
    .groupBy("dept_name")
    .agg(
        F.countDistinct("dept_name").alias("distinct_dept_names"),
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list_2"),
    )
    .filter(F.col("distinct_dept_names") > 1)
)
