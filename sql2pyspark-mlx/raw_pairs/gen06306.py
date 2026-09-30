from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title", "manager_id").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("job_title").orderBy(F.col("hire_date").desc(), "id")

result = (
    df_employees
    .filter(F.col("salary") >= 103000)
    .select(
        "id",
        "job_title",
        "manager_id",
        "hire_date",
        "salary",
        F.sum("salary").over(w).alias("running_salary"),
        F.row_number().over(w2).alias("rn"),
    )
)
