from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("order_date", "id")

result = (
    df_orders
    .filter(F.col("shipping_fee") < 19)
    .select(
        "id",
        "order_date",
        "shipping_fee",
        F.lag("shipping_fee", 1).over(w).alias("prev_shipping_fee"),
    )
)
