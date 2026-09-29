from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("order_date", "id")
w2 = Window.partitionBy("customer_id").orderBy(F.col("shipping_fee").desc(), "id")

result = (
    df_orders
    .filter(F.col("discount") <= 0.11)
    .select(
        "id",
        "order_date",
        "amount",
        "customer_id",
        "shipping_fee",
        F.lag("amount", 1).over(w).alias("prev_amount"),
        F.row_number().over(w2).alias("rn"),
    )
)
