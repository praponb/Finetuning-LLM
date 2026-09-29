from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("order_date", "id")

result = (
    df_orders
    .select(
        "id",
        "customer_id",
        "order_date",
        "shipping_fee",
        F.sum("shipping_fee").over(w).alias("running_shipping_fee"),
    )
    .orderBy(F.col("order_date").desc())
)
