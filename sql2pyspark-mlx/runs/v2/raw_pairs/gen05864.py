from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("hire_date", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_employees
    .select(
        "id",
        "hire_date",
        "salary",
        F.round(F.avg("salary").over(w), 2).alias("salary_moving_avg_3"),
    )
    .orderBy(F.col("hire_date").desc(), "salary")
)
