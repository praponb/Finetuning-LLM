from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")

result = (
    df_orders
    .filter(F.col("shipping_fee").between(20, 24))
    .select(
        "id",
        "store_id",
        "amount",
        F.min("amount").over(w).alias("store_id_min_amount"),
    )
    .orderBy("amount")
)
