from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("hire_quarter")

result = (
    df_employees
    .filter(F.col("manager_id").isin([29, 30]))
    .groupBy(F.quarter("hire_date").alias("hire_quarter"))
    .agg(
        F.sum("salary").alias("total_salary"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_salary", F.sum("total_salary").over(w))
)
