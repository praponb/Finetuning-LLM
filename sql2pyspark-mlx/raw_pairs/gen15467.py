from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("level")

result = (
    df_employees
    .filter(F.year("hire_date") >= 2025)
    .groupBy("level")
    .agg(
        F.sum("salary").alias("total_salary"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_salary", F.sum("total_salary").over(w))
    .orderBy("level")
)
