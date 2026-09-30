from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_credit_limit").desc())

result = (
    df_customers
    .groupBy("country")
    .agg(F.sum("credit_limit").alias("total_credit_limit"))
    .withColumn("credit_limit_rank", F.dense_rank().over(w))
    .orderBy("total_credit_limit", "country")
    .limit(20)
)
