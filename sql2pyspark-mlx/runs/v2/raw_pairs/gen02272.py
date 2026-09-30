from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("amount").desc())
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_payments
    .select(
        "id",
        "order_id",
        "amount",
        "method",
        "paid_at",
        F.rank().over(w).alias("amount_rank"),
        F.round(F.avg("amount").over(w2), 2).alias("amount_moving_avg_4"),
    )
)
