from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_credit_limit").desc())

result = (
    df_customers
    .groupBy("segment")
    .agg(
        F.sum("credit_limit").alias("total_credit_limit"),
        F.count("*").alias("cnt"),
    )
    .withColumn("credit_limit_rank", F.dense_rank().over(w))
    .orderBy(F.col("credit_limit_rank").desc())
)
