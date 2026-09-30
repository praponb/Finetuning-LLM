from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("status").orderBy("amount")

result = (
    df_payments
    .select(
        "id",
        "method",
        "paid_at",
        "amount",
        "status",
        F.round(F.avg("amount").over(w), 2).alias("amount_moving_avg_3"),
        F.dense_rank().over(w2).alias("dense_rnk"),
    )
)
