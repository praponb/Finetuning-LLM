from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.orderBy("shipping_fee", "id")

result = (
    df_orders
    .filter(F.col("channel").isin(["partner", "store"]))
    .select(
        "id",
        "status",
        "amount",
        "shipping_fee",
        "store_id",
        F.max("amount").over(w).alias("status_max_amount"),
        F.first("store_id").over(w2).alias("first_store_id"),
    )
    .orderBy(F.col("shipping_fee").desc())
)
