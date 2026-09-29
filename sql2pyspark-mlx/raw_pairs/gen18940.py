from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("discount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w2 = Window.partitionBy("store_id", "channel").orderBy("order_date", "id")

result = (
    df_orders
    .filter(F.col("shipping_fee") < 32)
    .select(
        "id",
        "store_id",
        "discount",
        "customer_id",
        "channel",
        "order_date",
        "shipping_fee",
        F.last("customer_id").over(w).alias("last_customer_id"),
        F.lag("shipping_fee", 2).over(w2).alias("prev_shipping_fee"),
    )
)
