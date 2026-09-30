from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("hire_date", "id")
w2 = Window.orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("job_title").orderBy(F.col("salary").desc())
w4 = Window.partitionBy("manager_id", "job_title").orderBy(F.col("salary").desc(), "id")
w5 = Window.partitionBy("manager_id")
w6 = Window.partitionBy("employee_job_title").orderBy(F.col("employee_hire_date").desc(), "id")
w7 = Window.partitionBy("employee_level").orderBy("latest_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("employee_level")
w_recent = Window.partitionBy("manager_id", "dept_id").orderBy("salary_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("level").orderBy("salary_bucket", "id")
salary_by_dept_id = df_employees.groupBy("dept_id").agg(F.avg("salary").alias("avg_employee_salary"))

base = (
    df_employees
    .filter((F.col("hire_date") >= F.lit("2024-01-07").cast("date")) & F.col("level").isNotNull() & (F.lower(F.trim("full_name")) == "olivia garcia"))
    .withColumn("email_filled", F.coalesce("email", F.lit("unassigned")))
    .withColumn("salary_band", F.when(F.col("salary") >= 125000, "High").when(F.col("salary") >= 64000, "Medium").otherwise("Low"))
    .withColumn("level_clean", F.coalesce("level", F.lit("DIRECT")))
)

metrics = (
    base
    .select(
        "*",
        F.lag("salary", 2).over(w).alias("prev_salary"),
        F.last("level", ignorenulls=True).over(w2).alias("last_known_level"),
        F.percent_rank().over(w3).alias("salary_percent_rank"),
        F.ntile(5).over(w4).alias("salary_bucket"),
        F.round((F.col("salary") / F.sum("salary").over(w5)), 4).alias("salary_share"),
    )
)

with_dims = (
    metrics
    .alias("a")
    .join(df_employees.alias("p"), F.col("a.manager_id") == F.col("p.id"), "left")
    .select(
        "a.*",
        F.col("p.job_title").alias("employee_job_title"),
        F.col("p.level").alias("employee_level"),
        F.col("p.hire_date").alias("employee_hire_date"),
    )
)

ranked = (
    with_dims
    .withColumn("latest_rank", F.row_number().over(w6))
    .filter(F.col("latest_rank") == 1)
)

with_windows = (
    ranked
    .withColumn("running_salary_share", F.sum("salary_share").over(w7))
    .withColumn("employee_level_avg_salary_bucket", F.avg("salary_bucket").over(w8))
    .withColumn("cumulative_salary_percent_rank", F.sum("salary_percent_rank").over(w_recent))
    .withColumn("salary_bucket_bucket", F.ntile(5).over(w10))
)

result = (
    with_windows
    .join(salary_by_dept_id, on="dept_id", how="left")
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        F.col("avg_employee_salary"),
    )
)
