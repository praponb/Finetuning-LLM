from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("discount").asc_nulls_last())
w2 = Window.partitionBy("store_id").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_orders
    .withColumn("discount_dense_rank", F.dense_rank().over(w))
    .withColumn("first_known_channel", F.first("channel", ignorenulls=True).over(w2))
    .withColumn("running_shipping_fee", F.sum("shipping_fee").over(w3))
)
