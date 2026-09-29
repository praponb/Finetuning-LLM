from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("discount").desc())

result = (
    df_orders
    .filter(F.col("order_date") >= "2024-10-01")
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") <= 3)
    .orderBy(F.col("customer_id").desc())
)
