from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_payments
    .select(
        "id",
        "order_id",
        "paid_at",
        "amount",
        F.sum("amount").over(w).alias("running_amount"),
        F.round(F.avg("amount").over(w2), 2).alias("amount_moving_avg_7"),
    )
)
