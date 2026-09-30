from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "method").orderBy("paid_at", "id")

result = (
    df_payments
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "amount",
        (F.col("amount") - F.lag("amount", 1, 0).over(w)).alias("amount_change"),
    )
)
