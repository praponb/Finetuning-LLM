from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("order_date", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_orders
    .select(
        "id",
        "status",
        "order_date",
        "shipping_fee",
        F.round(F.avg("shipping_fee").over(w), 2).alias("shipping_fee_moving_avg_3"),
    )
)
