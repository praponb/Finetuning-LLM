from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("order_date", "id")

result = (
    df_orders
    .select(
        "id",
        "order_date",
        "shipping_fee",
        F.sum("shipping_fee").over(w).alias("running_shipping_fee"),
    )
)
