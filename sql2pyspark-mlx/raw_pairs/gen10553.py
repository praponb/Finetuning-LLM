from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id")
w2 = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("dept_name", "location").orderBy("budget", "id")
w4 = Window.orderBy("budget_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.orderBy("budget_bucket_times_next_budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("running_sum_budget_bucket_times_next_budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_departments
    .filter(F.col("dept_name").isNotNull() & (F.col("budget") >= 1252000))
    .select(
        "*",
        F.concat_ws("-", "location", "dept_name").alias("location_dept_name_key"),
        F.trim("location").alias("clean_location"),
    )
)

with_windows = (
    base
    .select(
        "id",
        "budget",
        "clean_location",
        F.lead("budget", 1, 0).over(w).alias("next_budget"),
        F.last("dept_name", ignorenulls=True).over(w2).alias("last_known_dept_name"),
        F.ntile(3).over(w3).alias("budget_bucket"),
    )
)

kept = (
    with_windows
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.level") == "L5"), "left_semi")
)

kept_departments = (
    kept
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.salary") >= 140000), "left_semi")
)

base_departments = (
    kept_departments
    .filter(F.col("budget_bucket").isNull())
    .select(
        "*",
        (F.col("budget_bucket") * F.col("next_budget")).alias("budget_bucket_times_next_budget"),
    )
)

metrics = (
    base_departments
    .select(
        "id",
        "budget",
        "clean_location",
        "last_known_dept_name",
        "budget_bucket",
        "budget_bucket_times_next_budget",
        F.sum("budget_bucket_times_next_budget").over(w4).alias("running_sum_budget_bucket_times_next_budget"),
        F.min("budget_bucket_times_next_budget").over(w4).alias("running_min_budget_bucket_times_next_budget"),
        F.max("budget_bucket_times_next_budget").over(w4).alias("running_max_budget_bucket_times_next_budget"),
    )
)

metrics_departments = (
    metrics
    .select(
        "*",
        F.min("budget").over(w_running).alias("running_min_budget"),
        F.max("budget").over(w_running).alias("running_max_budget"),
        F.avg("budget").over(w_running).alias("running_avg_budget"),
        F.sum("running_min_budget_bucket_times_next_budget").over(w6).alias("running_running_min_budget_bucket_times_next_budget"),
    )
)

result = (
    metrics_departments
    .select(
        "id",
        F.col("budget").alias("BUDGET"),
        F.col("running_max_budget").alias("RUNNING_MAX_BUDGET_VAL"),
    )
    .unpivot(
        ids=["id"],
        values=["BUDGET", "RUNNING_MAX_BUDGET_VAL"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
