from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_budget").desc())

result = (
    df_departments
    .groupBy("location")
    .agg(
        F.sum("budget").alias("total_budget"),
        F.count("*").alias("cnt"),
    )
    .withColumn("rnk", F.rank().over(w))
    .orderBy("total_budget", F.col("rnk").desc(), "location")
    .limit(1)
)
