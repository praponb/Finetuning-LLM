from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w2 = Window.partitionBy("store_id").orderBy("order_date", "id")

result = (
    df_orders
    .filter(F.col("shipping_fee") > 24)
    .select(
        "id",
        "channel",
        "amount",
        "customer_id",
        "store_id",
        "order_date",
        "discount",
        F.last("customer_id").over(w).alias("last_customer_id"),
        (F.col("discount") - F.lag("discount", 1).over(w2)).alias("discount_change"),
    )
)
