from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("hire_date", "id")

result = (
    df_employees
    .select(
        "id",
        "job_title",
        "hire_date",
        "salary",
        F.row_number().over(w).alias("hire_date_row"),
        F.sum("salary").over(w).alias("running_salary"),
    )
)
