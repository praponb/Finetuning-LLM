from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy(F.col("salary").desc(), "id")
w2 = Window.partitionBy("manager_id").orderBy("hire_date", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_employees
    .select(
        "id",
        "manager_id",
        "salary",
        "hire_date",
        F.ntile(3).over(w).alias("quartile"),
        F.round(F.avg("salary").over(w2), 2).alias("salary_moving_avg_7"),
    )
)
