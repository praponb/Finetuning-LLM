from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method", "status").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id")

result = (
    df_payments
    .select(
        "id",
        "method",
        "status",
        "paid_at",
        "amount",
        "order_id",
        F.lag("amount", 1, 0).over(w).alias("prev_amount"),
        F.sum("amount").over(w2).alias("order_id_sum_amount"),
    )
)
