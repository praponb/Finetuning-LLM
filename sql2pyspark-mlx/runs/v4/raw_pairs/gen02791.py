from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id")

cleaned = (
    df_orders
    .filter(F.col("store_id").isNotNull() & (F.col("order_date") < "2024-03-01"))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.lower("channel").alias("channel_lower"),
    )
)

result = (
    cleaned
    .withColumn("last_known_channel", F.last("channel", ignorenulls=True).over(w))
    .withColumn("corr_shipping_fee_amount", F.corr("shipping_fee", "amount").over(w2))
)
