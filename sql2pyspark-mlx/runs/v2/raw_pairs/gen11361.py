from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("order_year")

result = (
    df_orders
    .groupBy(F.year("order_date").alias("order_year"))
    .agg(F.sum("amount").alias("total_amount"))
    .withColumn("cumulative_amount", F.sum("total_amount").over(w))
    .orderBy(F.col("cumulative_amount").desc(), "order_year")
)
