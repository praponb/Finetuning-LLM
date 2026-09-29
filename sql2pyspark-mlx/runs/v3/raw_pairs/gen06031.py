from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.partitionBy("customer_id").orderBy("order_date", "id")

result = (
    df_orders
    .filter(F.col("channel").isin(["store", "mobile"]))
    .select(
        "id",
        "store_id",
        "amount",
        "customer_id",
        "order_date",
        "shipping_fee",
        F.max("amount").over(w).alias("store_id_max_amount"),
        F.lag("shipping_fee", 1).over(w2).alias("prev_shipping_fee"),
    )
)
