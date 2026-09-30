from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("amount", "id").rowsBetween(-3, -1)
w2 = Window.partitionBy("status", "channel").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_orders
    .withColumn("shipping_fee_avg_3_prior", F.avg("shipping_fee").over(w))
    .withColumn("last_known_channel", F.last("channel", ignorenulls=True).over(w2))
    .withColumn("last_known_discount", F.last("discount", ignorenulls=True).over(w3))
)
