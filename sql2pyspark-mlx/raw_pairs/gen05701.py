from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title", "level").orderBy("hire_date", "id")
salary_by_dept_id = df_employees.groupBy("dept_id").agg(F.avg("salary").alias("avg_employee_salary"))

with_windows = (
    df_employees
    .withColumn("prev_salary", F.lag("salary").over(w))
    .withColumn("seq_no", F.row_number().over(w))
)

result = (
    with_windows
    .join(salary_by_dept_id, on="dept_id", how="left")
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        "prev_salary",
        F.col("avg_employee_salary"),
    )
    .orderBy(F.col("prev_salary").asc_nulls_last())
)
