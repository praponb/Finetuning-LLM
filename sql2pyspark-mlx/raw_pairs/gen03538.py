from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_payments
    .select(
        "id",
        "status",
        "paid_at",
        "order_id",
        "amount",
        F.dense_rank().over(w).alias("dense_rnk"),
        F.round(F.avg("amount").over(w2), 2).alias("amount_moving_avg_4"),
    )
)
