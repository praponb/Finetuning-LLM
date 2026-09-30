from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name", "location").orderBy(F.col("budget").desc_nulls_first(), "id")
w2 = Window.partitionBy("dept_name").orderBy("latest_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("dept_name", "location")
w_grp = Window.orderBy("running_min_budget_times_budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())

latest = (
    df_departments
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

metrics = (
    latest
    .select(
        "id",
        "budget",
        "latest_rank",
        F.avg("budget").over(w2).alias("running_avg_budget"),
        F.min("budget").over(w2).alias("running_min_budget"),
        F.max("budget").over(w2).alias("running_max_budget"),
        F.round((F.col("budget") / F.sum("budget").over(w3)), 4).alias("budget_share"),
    )
)

filtered = (
    metrics
    .filter(F.col("running_min_budget") > 2051000)
    .select(
        "*",
        (F.col("running_min_budget") * F.col("budget")).alias("running_min_budget_times_budget"),
    )
)

metrics_departments = (
    filtered
    .withColumn("running_count_running_max_budget", F.count("running_max_budget").over(w_grp))
    .withColumn("running_avg_running_max_budget", F.avg("running_max_budget").over(w_grp))
    .withColumn("running_sum_running_max_budget", F.sum("running_max_budget").over(w_grp))
    .withColumn("running_min_running_max_budget", F.min("running_max_budget").over(w_grp))
)

with_latest = (
    metrics_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.budget",
        "s.running_avg_budget",
        "s.running_max_budget",
        "s.budget_share",
        "s.running_min_budget_times_budget",
        "s.running_count_running_max_budget",
        "s.running_avg_running_max_budget",
        "s.running_sum_running_max_budget",
        F.col("c.job_title").alias("employee_job_title"),
        F.col("c.hire_date").alias("employee_hire_date"),
    )
)

result = (
    with_latest
    .groupBy(F.date_trunc("QUARTER", "employee_hire_date").alias("quarter_start"))
    .agg(
        F.count(F.when(F.col("employee_job_title") == "Intern", 1)).alias("intern_rows"),
        F.count_if(F.col("employee_job_title") == "Engineer").alias("engineer_events"),
        F.max("budget_share").alias("max_budget_share"),
    )
    .orderBy(F.col("intern_rows").desc_nulls_first())
)
