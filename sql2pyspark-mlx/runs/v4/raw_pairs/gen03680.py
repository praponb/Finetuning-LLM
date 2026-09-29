from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at")
w2 = Window.partitionBy("status")

result = (
    df_payments
    .select(
        "id",
        "order_id",
        "paid_at",
        "status",
        "amount",
        F.rank().over(w).alias("rnk"),
        F.sum("amount").over(w2).alias("status_sum_amount"),
    )
    .orderBy("paid_at", "status_sum_amount")
)
