from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("amount").desc())

result = (
    df_orders
    .filter(F.year("order_date") >= 2025)
    .withColumn("row_num", F.dense_rank().over(w))
    .filter(F.col("row_num") <= 3)
    .select("id", "store_id", "amount", "customer_id")
)
