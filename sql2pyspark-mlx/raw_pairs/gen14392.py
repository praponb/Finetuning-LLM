from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("shipping_fee", "id")

result = (
    df_orders
    .filter(F.col("shipping_fee").between(24, 36))
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "channel", "shipping_fee")
)
