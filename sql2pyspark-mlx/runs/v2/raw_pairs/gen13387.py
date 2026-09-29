from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level")
w2 = Window.orderBy("hire_date", "id")

result = (
    df_employees
    .filter(F.col("salary") < 63000)
    .select(
        "id",
        "level",
        "salary",
        "hire_date",
        F.sum("salary").over(w).alias("level_sum_salary"),
        F.sum("salary").over(w2).alias("running_salary"),
    )
)
