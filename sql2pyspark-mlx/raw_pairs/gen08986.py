from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.partitionBy("channel").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_orders
    .select(
        "id",
        "customer_id",
        "amount",
        "channel",
        "shipping_fee",
        F.avg("amount").over(w).alias("customer_id_avg_amount"),
        F.last("shipping_fee").over(w2).alias("last_shipping_fee"),
    )
)
