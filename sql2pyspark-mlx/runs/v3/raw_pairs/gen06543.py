from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("paid_at", "id")

result = (
    df_payments
    .filter(F.col("order_id").isin([6, 15, 22]))
    .select(
        "id",
        "paid_at",
        "amount",
        F.lag("amount", 1, 0).over(w).alias("prev_amount"),
        (F.col("amount") - F.lag("amount", 1).over(w)).alias("amount_change"),
    )
)
