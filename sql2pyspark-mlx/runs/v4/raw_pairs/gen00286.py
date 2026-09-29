from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("discount", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("status").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("amount", "id")
w4 = Window.orderBy("discount", "id")

result = (
    df_orders
    .withColumn("shipping_fee_avg_3", F.avg("shipping_fee").over(w))
    .withColumn("first_known_shipping_fee", F.first("shipping_fee", ignorenulls=True).over(w2))
    .withColumn("next_shipping_fee", F.lead("shipping_fee", 2, 0).over(w3))
    .withColumn("discount_bucket", F.ntile(5).over(w4))
)
