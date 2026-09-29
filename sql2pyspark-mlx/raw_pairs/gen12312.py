from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_payments
    .filter(F.col("order_id") == 35)
    .select(
        "id",
        "order_id",
        "paid_at",
        "amount",
        F.round(F.avg("amount").over(w), 2).alias("amount_moving_avg_3"),
    )
    .orderBy("paid_at")
)
