from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_employees
    .filter(F.col("manager_id") == 6)
    .select(
        "id",
        "dept_id",
        "hire_date",
        "salary",
        F.sum("salary").over(w).alias("running_salary"),
    )
)
