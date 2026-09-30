from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("hire_date", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.orderBy(F.col("salary").desc())

result = (
    df_employees
    .select(
        "id",
        "manager_id",
        "hire_date",
        "salary",
        F.round(F.avg("salary").over(w), 2).alias("salary_moving_avg_7"),
        F.dense_rank().over(w2).alias("salary_dense_rank"),
    )
)
