from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("status", "channel")
w2 = Window.partitionBy("customer_id").orderBy("amount", "id")
w3 = Window.partitionBy("status")

enriched = (
    df_orders
    .withColumn("status_avg_shipping_fee", F.avg("shipping_fee").over(w_hist))
    .withColumn("amount_bucket", F.ntile(4).over(w2))
    .withColumn("status_avg_amount", F.avg("amount").over(w3))
)

result = (
    enriched
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
    )
    .orderBy(F.col("order_date").desc(), "amount")
)
