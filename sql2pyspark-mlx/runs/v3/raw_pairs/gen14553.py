from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("shipping_fee", "id")
w2 = Window.orderBy("order_date", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_orders
    .select(
        "id",
        "store_id",
        "shipping_fee",
        "order_date",
        "amount",
        F.row_number().over(w).alias("rn"),
        F.round(F.avg("amount").over(w2), 2).alias("amount_moving_avg_7"),
    )
)
