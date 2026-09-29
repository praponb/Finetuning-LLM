from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("method", "order_id").orderBy("paid_at", "id")

result = (
    df_payments
    .select(
        "id",
        "method",
        "paid_at",
        "amount",
        "order_id",
        F.round(F.avg("amount").over(w), 2).alias("amount_moving_avg_3"),
        F.ntile(3).over(w2).alias("quartile"),
    )
    .orderBy("paid_at")
)
