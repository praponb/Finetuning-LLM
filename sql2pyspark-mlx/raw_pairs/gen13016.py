from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("dept_id", "job_title").orderBy("hire_date", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_employees
    .filter(F.col("hire_date") >= "2024-02-01")
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "job_title",
        "salary",
        "level",
        F.min("salary").over(w_recent).alias("last4_min_salary"),
        F.sum("salary").over(w_recent).alias("last4_sum_salary"),
        F.avg("salary").over(w_recent).alias("last4_avg_salary"),
    )
)
