from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("order_date", "id")

result = (
    df_orders
    .select(
        "id",
        "status",
        "order_date",
        "shipping_fee",
        "amount",
        (F.col("shipping_fee") - F.lag("shipping_fee", 2, 0).over(w)).alias("shipping_fee_change"),
        F.lag("amount", 2).over(w).alias("prev_amount"),
    )
    .orderBy("shipping_fee")
)
