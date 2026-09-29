from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "dept_id")
w2 = Window.partitionBy("job_title").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
salary_by_manager_id = df_employees.groupBy("manager_id").agg(F.sum("salary").alias("sum_employee_salary"))

with_windows = (
    df_employees
    .withColumn("salary_share", F.round((F.col("salary") / F.sum("salary").over(w)), 4))
    .withColumn("first_known_level", F.first("level", ignorenulls=True).over(w2))
)

result = (
    with_windows
    .join(salary_by_manager_id, on="manager_id", how="left")
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        "salary_share",
        F.col("sum_employee_salary"),
    )
    .orderBy(F.col("manager_id").desc_nulls_first())
)
