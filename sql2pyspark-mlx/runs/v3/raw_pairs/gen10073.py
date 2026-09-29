from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("order_date", "id")
w2 = Window.partitionBy("channel", "customer_id").orderBy("order_date", "id")

result = (
    df_orders
    .select(
        "id",
        "customer_id",
        "order_date",
        "amount",
        "channel",
        "shipping_fee",
        F.lag("amount", 2, 0).over(w).alias("prev_amount"),
        (F.col("shipping_fee") - F.lag("shipping_fee", 2).over(w2)).alias("shipping_fee_change"),
    )
)
