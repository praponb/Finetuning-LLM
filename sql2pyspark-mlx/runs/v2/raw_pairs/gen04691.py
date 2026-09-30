from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id")

result = (
    df_payments
    .select(
        "id",
        "method",
        "paid_at",
        "amount",
        "order_id",
        (F.col("amount") - F.lag("amount", 1).over(w)).alias("amount_change"),
        F.sum("amount").over(w2).alias("order_id_sum_amount"),
    )
)
