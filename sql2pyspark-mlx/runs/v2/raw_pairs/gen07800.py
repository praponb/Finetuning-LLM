from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_age").desc())

result = (
    df_customers
    .groupBy("city")
    .agg(F.sum("age").alias("total_age"))
    .withColumn("rnk", F.rank().over(w))
    .orderBy(F.col("rnk").desc(), F.col("city").desc())
)
