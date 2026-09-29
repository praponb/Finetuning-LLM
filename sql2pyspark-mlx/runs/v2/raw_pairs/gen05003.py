from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_salary").desc())

result = (
    df_employees
    .filter(F.col("hire_date") < "2023-09-01")
    .groupBy("dept_id")
    .agg(F.sum("salary").alias("total_salary"))
    .withColumn("rnk", F.dense_rank().over(w))
    .orderBy("dept_id")
)
