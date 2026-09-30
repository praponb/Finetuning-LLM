from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_budget").desc())

result = (
    df_departments
    .filter(F.col("location").isNotNull())
    .groupBy("dept_name")
    .agg(F.sum("budget").alias("total_budget"))
    .withColumn("budget_rank", F.rank().over(w))
    .orderBy(F.col("total_budget").desc())
)
