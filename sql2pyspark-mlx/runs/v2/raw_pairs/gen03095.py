from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("order_date", "id")
w2 = Window.partitionBy("customer_id").orderBy(F.col("shipping_fee").desc())

result = (
    df_orders
    .filter(F.col("discount") > 0.11)
    .select(
        "id",
        "order_date",
        "shipping_fee",
        "customer_id",
        F.first("shipping_fee").over(w).alias("first_shipping_fee"),
        F.dense_rank().over(w2).alias("dense_rnk"),
    )
)
