from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("is_remote")

result = (
    df_employees
    .filter(F.col("hire_date").between("2023-10-29", "2024-01-01"))
    .groupBy("is_remote")
    .agg(F.sum("salary").alias("total_salary"))
    .withColumn("cumulative_salary", F.sum("total_salary").over(w))
    .orderBy(F.col("cumulative_salary").desc())
)
