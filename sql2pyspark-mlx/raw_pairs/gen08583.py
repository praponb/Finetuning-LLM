from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_payments
    .filter(F.col("order_id").isNull())
    .select(
        "id",
        "status",
        "paid_at",
        "amount",
        F.round(F.avg("amount").over(w), 2).alias("amount_moving_avg_3"),
    )
)
