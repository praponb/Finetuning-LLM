from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")

result = (
    df_orders
    .withColumn("min_shipping_fee_by_status", F.min("shipping_fee").over(w))
    .filter(F.col("shipping_fee") == F.col("min_shipping_fee_by_status"))
    .select("id", "status", "shipping_fee", "discount")
    .orderBy("id")
)
