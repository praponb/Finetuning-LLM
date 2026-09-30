from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("discount", "id")
w2 = Window.partitionBy("store_id").orderBy("amount", "id")

result = (
    df_orders
    .select(
        "id",
        "order_date",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.lag("discount", 1).over(w).alias("prev_discount"),
        F.ntile(4).over(w2).alias("amount_bucket"),
    )
)
