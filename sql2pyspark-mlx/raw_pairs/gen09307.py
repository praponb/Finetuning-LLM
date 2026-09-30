from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("dept_id", "level").orderBy("hire_date", "id")

result = (
    df_employees
    .select(
        "id",
        "full_name",
        "dept_id",
        "hire_date",
        F.first("job_title", ignorenulls=True).over(w).alias("first_known_job_title"),
        F.sum("salary").over(w2).alias("cumulative_salary"),
        F.row_number().over(w_grp).alias("event_seq"),
    )
)
