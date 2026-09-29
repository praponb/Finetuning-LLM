from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id", "level").orderBy("salary")
w2 = Window.partitionBy("job_title", "dept_id").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("level").orderBy("salary", "id").rowsBetween(-2, Window.currentRow)
salary_by_dept_id = df_employees.groupBy("dept_id").agg(F.min("salary").alias("min_employee_salary"))

scored = (
    df_employees
    .filter(F.col("dept_id") == 16)
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        "job_title",
        "level",
        F.dense_rank().over(w).alias("salary_dense_rank"),
        F.sum("salary").over(w2).alias("cumulative_salary"),
        F.avg("salary").over(w3).alias("salary_avg_2"),
    )
)

result = (
    scored
    .join(salary_by_dept_id, on="dept_id", how="left")
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        "job_title",
        "level",
        "salary_dense_rank",
        "salary_avg_2",
        F.when(F.col("full_name").rlike("^(Henry Ford|bob smith)$"), "CORE").otherwise("PRIORITY").alias("full_name_zone"),
        F.col("min_employee_salary"),
    )
)
