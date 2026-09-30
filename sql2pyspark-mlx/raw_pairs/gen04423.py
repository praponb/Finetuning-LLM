from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")

result = (
    df_payments
    .filter(F.col("status") == "REFUNDED")
    .select(
        "id",
        "order_id",
        "paid_at",
        "amount",
        F.lag("amount", 2, 0).over(w).alias("prev_amount"),
    )
)
