from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_employees
    .select(
        "id",
        "full_name",
        "dept_id",
        "job_title",
        "hire_date",
        "is_remote",
        F.sum("salary").over(w).alias("running_sum_salary"),
        F.avg("salary").over(w).alias("running_avg_salary"),
        F.max("salary").over(w).alias("running_max_salary"),
        F.count("salary").over(w).alias("running_count_salary"),
    )
)

result = (
    metrics
    .select("id", "full_name", "hire_date", "running_avg_salary", "running_max_salary")
    .orderBy("id", "running_max_salary")
)
