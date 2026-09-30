from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_budget").desc())

result = (
    df_departments
    .filter(F.col("location").isin(["London", "Chiang Mai", "Berlin"]))
    .groupBy(
        F.when(F.col("budget") >= 4351000, "high").when(F.col("budget") >= 1812000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.sum("budget").alias("total_budget"))
    .withColumn("budget_rank", F.rank().over(w))
)
