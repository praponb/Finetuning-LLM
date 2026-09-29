from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").desc())
w_recent = Window.partitionBy("dept_name").orderBy("dedup_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("location").orderBy(F.col("budget").asc_nulls_first())
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w5 = Window.partitionBy("job_title").orderBy("budget", "id")
w6 = Window.partitionBy("job_title").orderBy("running_budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_departments
    .filter(~F.col("dept_name").isin(["Marketing", "HR"]))
    .withColumn("dept_name_title", F.initcap(F.trim("dept_name")))
    .withColumn("location_clean", F.coalesce("location", F.lit("DIRECT")))
)

latest = (
    base
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

enriched = (
    latest
    .select(
        "*",
        F.sum("budget").over(w_recent).alias("running_budget"),
        F.cume_dist().over(w3).alias("budget_cume_dist"),
        F.last("dept_name", ignorenulls=True).over(w_recent).alias("last_known_dept_name"),
    )
)

with_last = (
    enriched
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.budget",
        "s.running_budget",
        F.col("c.salary").alias("employee_salary"),
        F.col("c.job_title"),
    )
)

kept = (
    with_last
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.salary") <= 115000), "left_semi")
)

eligible = (
    kept
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.salary") < 85000), "left_anti")
)

result = (
    eligible
    .filter(F.col("job_title") != "Manager")
    .select(
        "id",
        "budget",
        "running_budget",
        "employee_salary",
        "job_title",
        F.lag("running_budget").over(w5).alias("prev_running_budget"),
        F.lead("running_budget").over(w5).alias("next_running_budget"),
        F.lag("budget").over(w5).alias("prev_budget"),
        F.lead("budget").over(w5).alias("next_budget"),
        F.row_number().over(w5).alias("seq_no"),
        F.sum("employee_salary").over(w6).alias("cumulative_employee_salary"),
    )
)
