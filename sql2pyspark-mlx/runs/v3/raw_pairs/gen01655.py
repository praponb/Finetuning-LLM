from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("order_date", "id")

result = (
    df_orders
    .select(
        "id",
        "status",
        "order_date",
        "shipping_fee",
        F.lag("shipping_fee", 1).over(w).alias("prev_shipping_fee"),
    )
)
