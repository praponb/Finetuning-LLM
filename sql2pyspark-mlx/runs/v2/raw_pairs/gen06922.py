from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_age").desc())

result = (
    df_customers
    .filter(F.col("is_active"))
    .groupBy("segment")
    .agg(F.sum("age").alias("total_age"))
    .withColumn("age_rank", F.rank().over(w))
    .orderBy("segment")
)
