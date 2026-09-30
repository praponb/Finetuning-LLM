from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("order_date", "id")
w2 = Window.orderBy("order_date", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_orders
    .select(
        "id",
        "status",
        "order_date",
        "amount",
        F.lag("amount", 1, 0).over(w).alias("prev_amount"),
        F.round(F.avg("amount").over(w2), 2).alias("amount_moving_avg_4"),
    )
)
