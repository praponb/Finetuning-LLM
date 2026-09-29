from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_age").desc())

result = (
    df_customers
    .groupBy(F.date_format("signup_date", "yyyy-MM").alias("signup_month"))
    .agg(F.sum("age").alias("total_age"))
    .withColumn("age_rank", F.dense_rank().over(w))
)
