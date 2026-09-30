from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_revenue").desc())

result = (
    df_store_sales
    .groupBy(F.date_trunc("MONTH", "sale_date").alias("sale_month"))
    .agg(F.sum("revenue").alias("total_revenue"))
    .withColumn("rnk", F.dense_rank().over(w))
    .orderBy("rnk")
)
