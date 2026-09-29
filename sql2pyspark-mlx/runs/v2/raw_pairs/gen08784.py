from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("is_remote")

result = (
    df_employees
    .filter(F.col("hire_date") >= "2023-07-01")
    .groupBy("is_remote")
    .agg(
        F.sum("salary").alias("total_salary"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_salary", F.sum("total_salary").over(w))
)
