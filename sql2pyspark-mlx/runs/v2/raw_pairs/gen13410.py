from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.partitionBy("store_id")

result = (
    df_orders
    .select(
        "id",
        "status",
        "amount",
        "store_id",
        "shipping_fee",
        F.round((F.col("amount") / F.sum("amount").over(w)), 3).alias("amount_share"),
        F.sum("shipping_fee").over(w2).alias("store_id_sum_shipping_fee"),
    )
    .orderBy("status")
)
