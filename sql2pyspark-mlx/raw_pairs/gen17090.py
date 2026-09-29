from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name")
w2 = Window.orderBy("budget", "id")
w3 = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("dept_name").orderBy(F.col("cumulative_budget").desc(), "id")
w5 = Window.orderBy("cumulative_budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_departments
    .filter(F.col("location").isNotNull())
    .withColumn("location_lower", F.lower("location"))
    .withColumn("dept_name_group", F.when(F.col("dept_name").isin(["Legal", "Sales"]), "Priority").otherwise("Other"))
)

scored = (
    filtered
    .withColumn("dept_name_count_budget", F.count("budget").over(w))
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("cumulative_budget", F.sum("budget").over(w3))
)

with_windows = (
    scored
    .select(
        "id",
        "dept_name",
        "location",
        "dept_name_group",
        "cumulative_budget",
        F.ntile(3).over(w4).alias("cumulative_budget_bucket"),
        F.sum("cumulative_budget").over(w5).alias("running_cumulative_budget"),
    )
)

filtered_departments = (
    with_windows
    .filter(F.col("running_cumulative_budget") != 39571389)
    .withColumn("running_cumulative_budget_whole", F.ceil("running_cumulative_budget"))
)

grouped = (
    filtered_departments
    .withColumn("running_cumulative_budget_tier", F.when(F.col("running_cumulative_budget") >= 95195832, "HIGH").otherwise("LOW"))
    .withColumn("cumulative_budget_bucket_tier", F.when(F.col("cumulative_budget_bucket") >= 8, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["running_cumulative_budget_tier", "cumulative_budget_bucket_tier"], ["running_cumulative_budget_tier"], []],
        "running_cumulative_budget_tier",
        "cumulative_budget_bucket_tier",
    )
    .agg(
        F.percentile_approx("running_cumulative_budget_whole", 0.5).alias("running_cumulative_budget_whole_p50"),
        F.sum(F.when(F.col("dept_name") == "Sales", F.col("cumulative_budget_bucket")).otherwise(0)).alias("sales_cumulative_budget_bucket"),
        F.size(F.collect_set("dept_name")).alias("num_dept_names"),
    )
    .filter(F.col("num_dept_names") > 1)
)

result = (
    grouped
    .select(
        "running_cumulative_budget_tier",
        "cumulative_budget_bucket_tier",
        "running_cumulative_budget_whole_p50",
        "sales_cumulative_budget_bucket",
    )
)
