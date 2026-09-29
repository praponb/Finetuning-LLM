from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("location")
w3 = Window.partitionBy("location").orderBy("budget", "id")
w_ordered = Window.partitionBy("dept_name").orderBy("budget", "id")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w6 = Window.partitionBy("location").orderBy("budget_bucket_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("location").orderBy("budget_avg_3", "id").rowsBetween(-2, Window.currentRow)
w_grp = Window.partitionBy("dept_name").orderBy("budget_avg_3", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("location").orderBy("budget_avg_3", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("location").orderBy("location_max_budget", "id").rowsBetween(-6, Window.currentRow)

filtered = (
    df_departments
    .filter(F.col("dept_name").isNotNull() & (~F.col("location").eqNullSafe(F.lit("Chiang Mai"))))
    .withColumn("dept_name_group", F.when(F.col("dept_name").isin(["Engineering", "Finance"]), "Tier 1").otherwise("Other"))
    .withColumn("location_filled", F.coalesce("location", F.lit("N/A")))
)

with_windows = (
    filtered
    .filter(F.col("location").isin(["London", "Bangkok", "Tokyo"]))
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        F.avg("budget").over(w).alias("budget_avg_3"),
        F.max("budget").over(w2).alias("location_max_budget"),
        F.ntile(5).over(w3).alias("budget_bucket"),
        F.ntile(5).over(w_ordered).alias("budget_bucket_2"),
    )
)

kept = (
    with_windows
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (~F.col("ch.job_title").isin(["Intern", "Engineer", "Manager"])), "left_anti")
)

with_latest = (
    kept
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.salary"))
)

enriched = (
    with_latest
    .select(
        "id",
        "location",
        "budget_avg_3",
        "location_max_budget",
        "salary",
        F.sum("location_max_budget").over(w6).alias("running_location_max_budget"),
        F.avg("budget_bucket_2").over(w7).alias("budget_bucket_2_avg_2"),
        F.last("budget_avg_3", ignorenulls=True).over(w_grp).alias("last_known_budget_avg_3"),
        F.sum("location_max_budget").over(w9).alias("running_location_max_budget_2"),
    )
)

result = (
    enriched
    .select(
        "id",
        "location",
        "budget_avg_3",
        "location_max_budget",
        "running_location_max_budget",
        "last_known_budget_avg_3",
        "running_location_max_budget_2",
        F.min("salary").over(w_running).alias("last7_min_salary"),
        F.max("salary").over(w_running).alias("last7_max_salary"),
        F.sum("salary").over(w_running).alias("last7_sum_salary"),
        F.avg("salary").over(w_running).alias("last7_avg_salary"),
    )
)
