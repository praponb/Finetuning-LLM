from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("hire_date").desc())
w2 = Window.partitionBy("level").orderBy("hire_date", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_employees
    .filter(F.col("salary") < 67000)
    .select(
        "id",
        "hire_date",
        "level",
        "salary",
        F.rank().over(w).alias("rnk"),
        F.round(F.avg("salary").over(w2), 2).alias("salary_moving_avg_4"),
    )
)
