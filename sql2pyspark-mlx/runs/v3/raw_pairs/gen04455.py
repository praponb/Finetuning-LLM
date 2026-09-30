from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_budget").desc())

result = (
    df_departments
    .filter(F.col("budget") <= 3523000)
    .groupBy("dept_name")
    .agg(
        F.sum("budget").alias("total_budget"),
        F.count("*").alias("cnt"),
    )
    .withColumn("budget_rank", F.dense_rank().over(w))
    .orderBy(F.col("total_budget").desc())
)
