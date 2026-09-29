from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_budget").desc())

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3596000, "high").when(F.col("budget") >= 1126000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.sum("budget").alias("total_budget"))
    .withColumn("rnk", F.rank().over(w))
)
