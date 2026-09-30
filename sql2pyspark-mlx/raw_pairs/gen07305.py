from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("order_date", "id")
w2 = Window.partitionBy("customer_id").orderBy("amount", "id").rowsBetween(-3, -1)
w3 = Window.partitionBy("channel").orderBy("order_date", "id")

result = (
    df_orders
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        F.lead("shipping_fee", 1).over(w).alias("next_shipping_fee"),
        F.avg("discount").over(w2).alias("discount_avg_3_prior"),
        F.lag("shipping_fee", 1).over(w3).alias("prev_shipping_fee"),
    )
)
