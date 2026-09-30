from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("month", "hire_date")).orderBy("salary")
w2 = Window.partitionBy("job_title").orderBy("salary")
w3 = Window.partitionBy("manager_id").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("dept_name").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("location", "dept_id").orderBy("budget", "id")

base = (
    df_employees
    .filter(F.col("hire_date") >= F.lit("2023-03-11").cast("date"))
    .withColumn("hire_date_plus_37d", F.date_add("hire_date", 37))
    .withColumn("job_title_lower", F.lower("job_title"))
)

with_dims = (
    base
    .alias("s")
    .join(df_employees.alias("b"), F.col("s.manager_id") == F.col("b.id"), "left")
    .filter(F.col("b.is_remote"))
    .select(
        "s.*",
        F.col("b.salary").alias("employee_salary"),
        F.col("b.full_name").alias("employee_full_name"),
    )
)

metrics = (
    with_dims
    .select(
        "id",
        "full_name",
        "dept_id",
        "salary",
        "hire_date",
        "level",
        "hire_date_plus_37d",
        "job_title_lower",
        F.cume_dist().over(w).alias("salary_cume_dist"),
        F.rank().over(w2).alias("salary_rank"),
        F.last("level", ignorenulls=True).over(w3).alias("last_known_level"),
    )
)

with_dims_employees = (
    metrics
    .alias("src")
    .join(df_departments.alias("d"), F.col("src.dept_id") == F.col("d.id"), "inner")
    .select(
        "src.id",
        "src.full_name",
        "src.dept_id",
        "src.hire_date",
        "src.job_title_lower",
        "src.salary_rank",
        F.col("d.dept_name"),
        F.col("d.budget"),
        F.col("d.location"),
    )
)

scored = (
    with_dims_employees
    .withColumn("last_known_dept_name", F.last("dept_name", ignorenulls=True).over(w4))
    .withColumn("prev_budget", F.lag("budget", 1).over(w5))
)

summary = (
    scored
    .groupBy(F.date_trunc("MONTH", "hire_date").alias("activity_month"))
    .agg(
        F.size(F.collect_set("location")).alias("num_locations"),
        F.percentile_approx("prev_budget", [0.25, 0.5, 0.75]).alias("prev_budget_quartiles"),
        F.count("*").alias("cnt"),
        F.avg(F.when(F.col("location") == "Chiang Mai", F.col("salary_rank"))).alias("chiang_mai_avg_salary_rank"),
    )
    .filter(F.col("num_locations") >= 1)
)

result = (
    summary
    .select(
        "activity_month",
        "num_locations",
        "prev_budget_quartiles",
        "cnt",
        "chiang_mai_avg_salary_rank",
    )
    .orderBy("activity_month")
)
