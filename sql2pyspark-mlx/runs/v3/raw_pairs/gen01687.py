from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_employees
    .select(
        "id",
        "level",
        "hire_date",
        "salary",
        F.sum("salary").over(w).alias("running_salary"),
    )
    .orderBy(F.col("running_salary").desc())
)
