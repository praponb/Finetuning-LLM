from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_age").desc())

result = (
    df_customers
    .filter(F.col("is_active"))
    .groupBy(F.date_trunc("MONTH", "signup_date").alias("signup_month"))
    .agg(F.sum("age").alias("total_age"))
    .withColumn("rnk", F.dense_rank().over(w))
    .orderBy("total_age")
)
