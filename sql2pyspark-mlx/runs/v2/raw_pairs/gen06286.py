from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("order_date", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_orders
    .filter(F.col("channel") == "partner")
    .select(
        "id",
        "customer_id",
        "order_date",
        "shipping_fee",
        F.round(F.avg("shipping_fee").over(w), 2).alias("shipping_fee_moving_avg_4"),
    )
    .orderBy("customer_id")
)
