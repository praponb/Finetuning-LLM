from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_salary").desc())

result = (
    df_employees
    .groupBy("dept_id")
    .agg(F.sum("salary").alias("total_salary"))
    .withColumn("salary_rank", F.rank().over(w))
    .orderBy("dept_id")
)
