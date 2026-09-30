from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("amount_with_shipping", "id").rowsBetween(-3, -1)
w_ordered = Window.orderBy("discount", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.orderBy("amount_with_shipping", "id")
w4 = Window.partitionBy("customer_id", "store_id").orderBy(F.col("shipping_fee").desc(), "id")
w5 = Window.orderBy("order_date", "id").rowsBetween(-5, -1)

filtered = (
    df_orders
    .filter(F.col("customer_id").isNotNull())
    .withColumn("amount_with_shipping", (F.col("amount") + F.col("shipping_fee")))
    .withColumn("resolved_channel", F.coalesce("channel", F.lit("N/A")))
)

result = (
    filtered
    .withColumn("amount_with_shipping_avg_3_prior", F.avg("amount_with_shipping").over(w))
    .withColumn("discount_avg_3", F.avg("discount").over(w_ordered))
    .withColumn("amount_with_shipping_bucket", F.ntile(3).over(w3))
    .withColumn("shipping_fee_bucket", F.ntile(3).over(w4))
    .withColumn("shipping_fee_avg_5_prior", F.avg("shipping_fee").over(w5))
)
