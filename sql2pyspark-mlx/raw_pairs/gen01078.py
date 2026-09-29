from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("order_date", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("channel").orderBy("order_date", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_orders
    .filter(F.col("discount") > 0.3)
    .select(
        "id",
        "status",
        "order_date",
        "shipping_fee",
        "channel",
        "amount",
        F.round(F.avg("shipping_fee").over(w), 2).alias("shipping_fee_moving_avg_7"),
        F.round(F.avg("amount").over(w2), 2).alias("amount_moving_avg_4"),
    )
    .orderBy(F.col("amount_moving_avg_4").desc(), "channel")
)
