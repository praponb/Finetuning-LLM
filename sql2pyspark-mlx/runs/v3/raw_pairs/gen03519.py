from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_payments
    .select(
        "id",
        "order_id",
        "paid_at",
        "amount",
        F.round(F.avg("amount").over(w), 2).alias("amount_moving_avg_4"),
    )
    .orderBy(F.col("order_id").desc())
)
