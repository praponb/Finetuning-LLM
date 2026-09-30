from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "store_id").orderBy(F.col("shipping_fee").desc(), "id")

result = (
    df_orders
    .select(
        "id",
        "channel",
        "store_id",
        "shipping_fee",
        "customer_id",
        F.first("customer_id").over(w).alias("first_customer_id"),
    )
)
