from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.partitionBy("channel").orderBy(F.col("amount").desc(), "id")

result = (
    df_orders
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "store_id",
        "shipping_fee",
        "channel",
        "amount",
        F.min("shipping_fee").over(w).alias("store_id_min_shipping_fee"),
        F.row_number().over(w2).alias("row_num"),
    )
)
