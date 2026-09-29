from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_employees
    .filter(F.col("hire_date").between("2024-01-01", "2024-09-01"))
    .select(
        "id",
        "level",
        "hire_date",
        "salary",
        F.sum("salary").over(w).alias("running_salary"),
    )
)
