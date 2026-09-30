from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("hire_date"), "job_title").orderBy(F.col("salary").desc(), "id")
w2 = Window.partitionBy("job_title").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("job_title")
w4 = Window.partitionBy("dept_id")
w5 = Window.partitionBy(F.month("hire_date"), "job_title").orderBy("salary")
salary_by_manager_id = df_employees.groupBy("manager_id").agg(F.max("salary").alias("max_employee_salary"))

scored = (
    df_employees
    .select(
        "id",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "is_remote",
        "level",
        F.ntile(10).over(w).alias("salary_bucket"),
        F.first("job_title", ignorenulls=True).over(w2).alias("first_known_job_title"),
        F.round((F.col("salary") / F.sum("salary").over(w3)), 4).alias("salary_share"),
        F.round((F.col("salary") / F.sum("salary").over(w4)), 4).alias("salary_share_2"),
        F.percent_rank().over(w5).alias("salary_percent_rank"),
    )
)

result = (
    scored
    .join(salary_by_manager_id, on="manager_id", how="left")
    .select(
        "id",
        "manager_id",
        "salary_bucket",
        "first_known_job_title",
        "salary_share",
        "salary_percent_rank",
        F.col("max_employee_salary"),
    )
    .orderBy("salary_bucket")
)
