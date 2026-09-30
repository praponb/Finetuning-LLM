from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "status").orderBy(F.col("discount").desc(), "id")
w2 = Window.orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_orders
    .withColumn("discount_bucket", F.ntile(10).over(w))
    .withColumn("last_known_shipping_fee", F.last("shipping_fee", ignorenulls=True).over(w2))
)
