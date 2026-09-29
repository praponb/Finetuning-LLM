from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("job_title").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_employees
    .select(
        "id",
        "full_name",
        "job_title",
        "salary",
        "is_remote",
        "level",
        F.sum("salary").over(w).alias("cumulative_salary"),
        F.first("job_title", ignorenulls=True).over(w2).alias("first_known_job_title"),
    )
)
