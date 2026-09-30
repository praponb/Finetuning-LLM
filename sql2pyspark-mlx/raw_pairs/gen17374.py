from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").desc(), "id")
w_hist = Window.partitionBy("dept_name").orderBy("budget_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("dept_name").orderBy("running_max_budget_filled", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("dept_name")

filtered = (
    df_departments
    .filter(F.col("budget") > 2429000)
    .select(
        "*",
        F.concat_ws("-", "location", "dept_name").alias("location_dept_name_key"),
    )
)

deduped = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "dept_name", "budget", "location_dept_name_key")
)

base = (
    deduped
    .filter((F.col("dept_name").eqNullSafe(F.lit("Finance"))) & F.col("dept_name").isin(["Legal", "Engineering"]))
    .select("*", F.coalesce("budget", F.lit(0)).alias("budget_filled"))
)

with_windows = (
    base
    .select(
        "id",
        "dept_name",
        "budget",
        "location_dept_name_key",
        F.sum("budget_filled").over(w_hist).alias("running_sum_budget_filled"),
        F.min("budget_filled").over(w_hist).alias("running_min_budget_filled"),
        F.max("budget_filled").over(w_hist).alias("running_max_budget_filled"),
        F.count("budget_filled").over(w_hist).alias("running_count_budget_filled"),
    )
)

with_windows_departments = (
    with_windows
    .select(
        "id",
        "dept_name",
        "budget",
        "location_dept_name_key",
        "running_min_budget_filled",
        "running_count_budget_filled",
        F.sum("running_count_budget_filled").over(w_part).alias("last7_sum_running_count_budget_filled"),
        F.count("running_count_budget_filled").over(w_part).alias("last7_count_running_count_budget_filled"),
        F.avg("running_count_budget_filled").over(w_part).alias("last7_avg_running_count_budget_filled"),
        F.round((F.col("budget") / F.sum("budget").over(w4)), 4).alias("budget_share"),
    )
)

result = (
    with_windows_departments
    .groupBy(
        F.when(F.col("running_min_budget_filled") >= 66, "LARGE").otherwise("SMALL").alias("running_min_budget_filled_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.avg("last7_count_running_count_budget_filled").alias("avg_last7_count_running_count_budget_filled"),
        F.round(F.avg("last7_sum_running_count_budget_filled"), 2).alias("avg_last7_sum_running_count_budget_filled"),
    )
    .orderBy(F.col("row_count").asc_nulls_first())
)
