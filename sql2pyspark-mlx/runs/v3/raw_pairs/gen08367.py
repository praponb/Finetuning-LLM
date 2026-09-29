from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("order_date", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_orders
    .filter(F.col("discount") >= 0.22)
    .select(
        "id",
        "order_date",
        "amount",
        F.round(F.avg("amount").over(w), 2).alias("amount_moving_avg_7"),
    )
    .orderBy("amount_moving_avg_7", "order_date")
)
