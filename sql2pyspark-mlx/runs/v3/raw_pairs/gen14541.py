from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_departments
    .filter(F.col("budget") >= 2490000)
    .groupBy(
        F.when(F.col("budget") >= 3092000, "high").when(F.col("budget") >= 1015000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum("budget").alias("total_budget"),
        F.count("*").alias("cnt"),
    )
    .withColumn("pct_of_total", F.round(((F.col("total_budget") * 100) / F.sum("total_budget").over(w)), 2))
    .orderBy(F.col("total_budget").desc(), F.col("pct_of_total").desc())
)
