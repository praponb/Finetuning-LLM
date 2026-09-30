from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")

result = (
    df_orders
    .select(
        "id",
        "status",
        "shipping_fee",
        "discount",
        F.avg("shipping_fee").over(w).alias("status_avg_shipping_fee"),
        F.min("discount").over(w).alias("status_min_discount"),
    )
)
