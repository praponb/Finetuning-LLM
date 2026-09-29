from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("credit_limit_bucket")

result = (
    df_customers
    .groupBy(
        F.when(F.col("credit_limit") >= 17900, "high").when(F.col("credit_limit") >= 6700, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(
        F.sum("age").alias("total_age"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_age", F.sum("total_age").over(w))
)
