from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("paid_at")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_payments
    .select(
        "id",
        "paid_at",
        "method",
        "amount",
        F.percent_rank().over(w).alias("paid_at_pct_rank"),
        F.round(F.avg("amount").over(w2), 2).alias("amount_moving_avg_7"),
    )
)
