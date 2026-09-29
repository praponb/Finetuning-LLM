from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("shipping_fee").desc(), "id")

result = (
    df_orders
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") <= 2)
    .select("id", "status", "shipping_fee")
    .orderBy("status", "shipping_fee")
)
