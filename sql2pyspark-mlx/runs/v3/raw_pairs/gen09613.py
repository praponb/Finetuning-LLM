from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("dept_name")

result = (
    df_departments
    .filter(F.col("budget") <= 3719000)
    .groupBy("dept_name")
    .agg(F.sum("budget").alias("total_budget"))
    .withColumn("cumulative_budget", F.sum("total_budget").over(w))
)
