from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy("hire_date", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_employees
    .select(
        "id",
        "dept_id",
        "hire_date",
        "salary",
        F.round(F.avg("salary").over(w), 2).alias("salary_moving_avg_3"),
    )
)
