from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("hire_month")

result = (
    df_employees
    .groupBy(F.date_trunc("MONTH", "hire_date").alias("hire_month"))
    .agg(F.sum("salary").alias("total_salary"))
    .withColumn("cumulative_salary", F.sum("total_salary").over(w))
    .orderBy(F.col("cumulative_salary").desc(), "total_salary", "hire_month")
    .limit(1)
)
