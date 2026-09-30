from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "customer_id").orderBy("order_date", "id")

result = (
    df_orders
    .filter(F.col("discount").between(0.2, 0.39))
    .select(
        "id",
        "store_id",
        "customer_id",
        "order_date",
        "shipping_fee",
        F.lag("shipping_fee", 1).over(w).alias("prev_shipping_fee"),
    )
)
