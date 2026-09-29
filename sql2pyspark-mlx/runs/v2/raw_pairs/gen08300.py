from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")

result = (
    df_orders
    .filter(F.year("order_date") == 2025)
    .select(
        "id",
        "status",
        "shipping_fee",
        F.max("shipping_fee").over(w).alias("status_max_shipping_fee"),
    )
)
