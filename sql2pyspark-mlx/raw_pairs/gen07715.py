from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("budget", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("level", "dept_id").orderBy("salary_rounded")
w3 = Window.partitionBy("manager_id", "dept_id").orderBy("salary", "id")
w4 = Window.partitionBy("dept_name").orderBy(F.col("next_budget").desc(), "id")
w5 = Window.partitionBy("dept_name").orderBy(F.col("next_budget_minus_salary").desc(), "id")
w_ordered = Window.partitionBy("dept_id").orderBy(F.col("hire_date_period").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w7 = Window.partitionBy("dept_id").orderBy("next_budget_minus_salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy(F.month("hire_date"), "dept_name")
salary_by_dept_id = df_employees.groupBy("dept_id").agg(F.sum("salary").alias("sum_employee_salary"))

filtered = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-02-15").cast("date")) & F.col("job_title").isNotNull())
    .withColumn("salary_rounded", F.round("salary", 2))
    .withColumn("level_filled", F.coalesce("level", F.lit("N/A")))
)

joined = (
    filtered
    .alias("src")
    .join(df_departments.alias("b"), F.col("src.dept_id") == F.col("b.id"), "inner")
    .select("src.*", F.col("b.budget"))
)

metrics = (
    joined
    .filter(F.col("hire_date") <= "2023-04-01")
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "salary",
        "hire_date",
        "salary_rounded",
        "level_filled",
        F.avg("budget").over(w).alias("budget_avg_2"),
        F.percent_rank().over(w2).alias("salary_rounded_percent_rank"),
        F.lead("budget", 1).over(w3).alias("next_budget"),
    )
)

joined_employees = (
    metrics
    .alias("s")
    .join(df_departments.alias("d"), F.col("s.dept_id") == F.col("d.id"), "left")
    .select(
        "s.id",
        "s.full_name",
        "s.dept_id",
        "s.salary",
        "s.hire_date",
        "s.next_budget",
        F.col("d.dept_name"),
    )
)

filtered_employees = (
    joined_employees
    .filter((F.col("hire_date") > F.lit("2023-05-11").cast("date")) & (F.col("hire_date") > "2024-04-01"))
    .select(
        "*",
        (F.col("next_budget") - F.col("salary")).alias("next_budget_minus_salary"),
        F.date_trunc("WEEK", "hire_date").alias("hire_date_period"),
    )
)

scored = (
    filtered_employees
    .select(
        "*",
        F.ntile(5).over(w4).alias("next_budget_bucket"),
        F.ntile(3).over(w5).alias("next_budget_minus_salary_bucket"),
        F.max("salary").over(w_ordered).alias("salary_max_7d"),
        F.last("dept_name", ignorenulls=True).over(w7).alias("last_known_dept_name"),
        F.sum("salary").over(w_recent).alias("hire_date_month_sum_salary"),
    )
)

result = (
    scored
    .join(salary_by_dept_id, on="dept_id", how="left")
    .select(
        "id",
        "dept_id",
        "salary",
        "next_budget",
        "dept_name",
        "next_budget_minus_salary",
        "hire_date_period",
        "next_budget_bucket",
        "next_budget_minus_salary_bucket",
        F.col("sum_employee_salary"),
    )
)
