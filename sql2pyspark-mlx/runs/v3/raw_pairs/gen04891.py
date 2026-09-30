from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("manager_id")

result = (
    df_employees
    .groupBy("manager_id")
    .agg(F.sum("salary").alias("total_salary"))
    .withColumn("cumulative_salary", F.sum("total_salary").over(w))
    .orderBy("manager_id")
    .limit(1)
)
