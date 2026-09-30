from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("hire_date").desc())
w2 = Window.orderBy("hire_date", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_employees
    .select(
        "id",
        "level",
        "hire_date",
        "salary",
        F.dense_rank().over(w).alias("hire_date_dense_rank"),
        F.round(F.avg("salary").over(w2), 2).alias("salary_moving_avg_3"),
    )
)
