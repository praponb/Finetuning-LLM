from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("order_date", "id")

result = (
    df_orders
    .select(
        "id",
        "channel",
        "order_date",
        "shipping_fee",
        F.lag("shipping_fee", 1, 0).over(w).alias("prev_shipping_fee"),
    )
)
