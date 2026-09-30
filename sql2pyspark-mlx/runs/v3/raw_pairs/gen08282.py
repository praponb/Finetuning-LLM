from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.orderBy(F.col("amount").desc(), "id")

result = (
    df_orders
    .select(
        "id",
        "store_id",
        "shipping_fee",
        "amount",
        F.min("shipping_fee").over(w).alias("store_id_min_shipping_fee"),
        F.ntile(10).over(w2).alias("quartile"),
    )
)
