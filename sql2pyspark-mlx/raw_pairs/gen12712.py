from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "manager_id").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("location", "dept_name").orderBy("salary")
w_running = Window.partitionBy("department_location").orderBy("budget", "id").rowsBetween(-3, Window.currentRow)
w_grp = Window.partitionBy(F.date_trunc("week", "hire_date"))

joined = (
    df_employees
    .alias("src")
    .join(df_departments.alias("p"), F.col("src.dept_id") == F.col("p.id"), "left")
    .filter(F.col("p.location").isin(["Chiang Mai", "Bangkok", "Berlin"]))
    .select("src.*", F.col("p.budget"), F.col("p.dept_name"), F.col("p.location"))
)

enriched = (
    joined
    .select(
        "*",
        F.sum("salary").over(w).alias("running_sum_salary"),
        F.min("salary").over(w).alias("running_min_salary"),
        F.max("salary").over(w).alias("running_max_salary"),
        F.percent_rank().over(w2).alias("salary_percent_rank"),
    )
)

with_dims = (
    enriched
    .alias("src")
    .join(df_departments.alias("p"), F.col("src.dept_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.location").alias("department_location"))
)

with_windows = (
    with_dims
    .select(
        "*",
        F.max("salary").over(w_running).alias("last4_max_salary"),
        F.sum("salary").over(w_running).alias("last4_sum_salary"),
        F.min("salary").over(w_running).alias("last4_min_salary"),
        F.avg("salary").over(w_running).alias("last4_avg_salary"),
        F.max("salary").over(w_grp).alias("hire_date_period_max_salary"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "dept_name",
        F.col("salary").alias("SALARY"),
        F.col("budget").alias("BUDGET"),
    )
    .unpivot(
        ids=["id", "dept_name"],
        values=["SALARY", "BUDGET"],
        variableColumnName="metric",
        valueColumnName="value",
    )
)
