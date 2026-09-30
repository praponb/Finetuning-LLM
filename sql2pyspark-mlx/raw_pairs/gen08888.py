from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("order_date", "id")
w2 = Window.partitionBy("store_id").orderBy("order_date", "id")

result = (
    df_orders
    .filter(F.col("discount").between(0.28, 0.36))
    .select(
        "id",
        "status",
        "order_date",
        "store_id",
        "shipping_fee",
        F.row_number().over(w).alias("row_num"),
        F.lag("shipping_fee", 1, 0).over(w2).alias("prev_shipping_fee"),
    )
)
