from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_salary").desc())

result = (
    df_employees
    .filter(F.col("manager_id") == 33)
    .groupBy(F.date_format("hire_date", "yyyy-MM").alias("hire_month"))
    .agg(F.sum("salary").alias("total_salary"))
    .withColumn("salary_rank", F.dense_rank().over(w))
    .orderBy("total_salary")
)
