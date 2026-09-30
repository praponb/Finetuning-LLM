from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_budget").desc())

result = (
    df_departments
    .groupBy("location")
    .agg(F.sum("budget").alias("total_budget"))
    .withColumn("budget_rank", F.dense_rank().over(w))
    .orderBy(F.col("total_budget").desc())
)
