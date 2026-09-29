from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("order_year")

result = (
    df_orders
    .groupBy(F.year("order_date").alias("order_year"))
    .agg(F.sum("discount").alias("total_discount"))
    .withColumn("cumulative_discount", F.sum("total_discount").over(w))
    .orderBy("order_year")
)
