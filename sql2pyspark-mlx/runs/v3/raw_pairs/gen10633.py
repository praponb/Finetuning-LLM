from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id", "job_title").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_employees
    .select(
        "id",
        "manager_id",
        "job_title",
        "hire_date",
        "salary",
        F.sum("salary").over(w).alias("running_salary"),
    )
)
