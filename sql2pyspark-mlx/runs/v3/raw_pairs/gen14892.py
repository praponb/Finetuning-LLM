from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("amount").desc(), "id")
w2 = Window.orderBy("order_date", "id")

result = (
    df_orders
    .select(
        "id",
        "amount",
        "order_date",
        "shipping_fee",
        F.row_number().over(w).alias("row_num"),
        F.lag("shipping_fee", 2).over(w2).alias("prev_shipping_fee"),
    )
)
