from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("order_date").desc())

result = (
    df_orders
    .withColumn("pos", F.rank().over(w))
    .filter(F.col("pos") <= 3)
    .select("id", "status", "order_date", "shipping_fee")
    .orderBy(F.col("status").desc())
)
